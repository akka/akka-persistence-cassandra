package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util._

import akka.actor._
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy

/**
 * Optimized and fully async version of [[akka.persistence.snapshot.SnapshotStore]].
 */
trait CassandraSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  final def receive = {
    case LoadSnapshot(processorId, criteria, toSequenceNr) ⇒
      val p = sender
      loadAsync(processorId, criteria.limit(toSequenceNr)) map {
        sso ⇒ LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e ⇒ LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) ⇒
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ ⇒ SaveSnapshotSuccess(md)
      } recover {
        case e ⇒ SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) ⇒
      deleteAsync(metadata) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(processorId, criteria) ⇒
      deleteAsync(processorId, criteria) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
  }

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]
  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]
  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

class CassandraSnapshotStore extends CassandraSnapshotStoreEndpoint with CassandraStatements with ActorLogging {
  import context.dispatcher

  val config = context.system.settings.config.getConfig("cassandra-snapshot-store")
  val keyspace = config.getString("keyspace")
  val table = config.getString("table")

  val maxMetadataResultSize = config.getInt("max-metadata-result-size")
  val serialization = SerializationExtension(context.system)

  val clusterBuilder = Cluster.builder.addContactPoints(config.getStringList("contact-points").asScala: _*)

  if(config.hasPath("port")) {
    clusterBuilder.withPort(config.getInt("port"))
  }

  if(config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password"))
  }

  if(config.hasPath("local-datacenter")) {
    clusterBuilder.withLoadBalancingPolicy(
      new DCAwareRoundRobinPolicy(config.getString("local-datacenter"))
    )
  }

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  val writeConsistency = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val readConsistency = ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val preparedWriteSnapshot = session.prepare(writeSnapshot).setConsistencyLevel(writeConsistency)
  val preparedDeleteSnapshot = session.prepare(deleteSnapshot).setConsistencyLevel(writeConsistency)
  val preparedSelectSnapshot = session.prepare(selectSnapshot).setConsistencyLevel(readConsistency)

  val preparedSelectSnapshotMetadataForLoad =
    session.prepare(selectSnapshotMetadata(limit = Some(maxMetadataResultSize))).setConsistencyLevel(readConsistency)

  val preparedSelectSnapshotMetadataForDelete =
    session.prepare(selectSnapshotMetadata(limit = None)).setConsistencyLevel(readConsistency)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    mds <- Future(metadata(processorId, criteria).take(3).toVector)
    res <- loadNAsync(mds)
  } yield res

  def loadNAsync(metadata: immutable.Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None)
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e => loadNAsync(mds) // try older snapshot
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val stmt = preparedSelectSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(rs => deserialize(rs.one().getBytes("snapshot")))
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val stmt = preparedWriteSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong, metadata.timestamp: JLong, serialize(Snapshot(snapshot)))
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val stmt = preparedDeleteSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = for {
    mds <- Future(metadata(processorId, criteria).toVector)
    res <- executeBatch(batch => mds.foreach(md => batch.add(preparedDeleteSnapshot.bind(md.processorId, md.sequenceNr: JLong))))
  } yield res

  def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def serialize(snapshot: Snapshot): ByteBuffer =
    ByteBuffer.wrap(serialization.findSerializerFor(snapshot).toBinary(snapshot))

  private def deserialize(bytes: ByteBuffer): Snapshot =
    serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get

  private def metadata(processorId: String, criteria: SnapshotSelectionCriteria): Iterator[SnapshotMetadata] =
    new RowIterator(processorId, criteria.maxSequenceNr).map { row =>
      SnapshotMetadata(row.getString("processor_id"), row.getLong("sequence_nr"), row.getLong("timestamp"))
    }.dropWhile(_.timestamp > criteria.maxTimestamp)

  private class RowIterator(processorId: String, maxSequenceNr: Long) extends Iterator[Row] {
    var currentSequenceNr = maxSequenceNr
    var rowCount = 0
    var iter = newIter()

    def newIter() = session.execute(selectSnapshotMetadata(Some(maxMetadataResultSize)), processorId, currentSequenceNr: JLong).iterator

    @annotation.tailrec
    final def hasNext: Boolean =
      if (iter.hasNext)
        true
      else if (rowCount < maxMetadataResultSize)
        false
      else {
        rowCount = 0
        currentSequenceNr -= 1
        iter = newIter()
        hasNext
      }

    def next(): Row = {
      val row = iter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      rowCount += 1
      row
    }
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }
}
