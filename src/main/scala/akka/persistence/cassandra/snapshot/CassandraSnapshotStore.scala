/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.utils.Bytes
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

class CassandraSnapshotStore(cfg: Config) extends SnapshotStore with CassandraStatements with ActorLogging {
  val config = new CassandraSnapshotStoreConfig(cfg)
  val serialization = SerializationExtension(context.system)

  import config._
  import context.dispatcher

  private[snapshot] class CassandraSession(val underlying: Session) {

    if (config.keyspaceAutoCreate)
      underlying.execute(createKeyspace)
    underlying.execute(createTable)

    val preparedWriteSnapshot = underlying.prepare(writeSnapshot).setConsistencyLevel(writeConsistency)
    val preparedDeleteSnapshot = underlying.prepare(deleteSnapshot).setConsistencyLevel(writeConsistency)
    val preparedSelectSnapshot = underlying.prepare(selectSnapshot).setConsistencyLevel(readConsistency)

    val preparedSelectSnapshotMetadataForLoad =
      underlying.prepare(selectSnapshotMetadata(limit = Some(maxMetadataResultSize))).setConsistencyLevel(readConsistency)

    val preparedSelectSnapshotMetadataForDelete =
      underlying.prepare(selectSnapshotMetadata(limit = None)).setConsistencyLevel(readConsistency)

  }

  private var sessionUsed = false

  private[this] lazy val cassandraSession: CassandraSession = {
    retry(config.connectionRetries + 1, config.connectionRetryDelay.toMillis) {
      val underlying: Session = clusterBuilder.build.connect()
      try {
        val s = new CassandraSession(underlying)
        log.debug("initialized CassandraSession successfully")
        sessionUsed = true
        s
      } catch {
        case NonFatal(e) =>
          // will be retried
          if (log.isDebugEnabled)
            log.debug("issue with initialization of CassandraSession, will be retried: {}", e.getMessage)
          closeSession(underlying)
          throw e
      }
    }
  }

  def session: Session = cassandraSession.underlying

  private def closeSession(session: Session): Unit = try {
    session.close()
    session.getCluster().close()
  } catch {
    case NonFatal(_) => // nothing we can do
  }

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraSnapshotStore.Init
  }

  override def receivePluginInternal: Receive = {
    case CassandraSnapshotStore.Init =>
      try {
        cassandraSession
      } catch {
        case NonFatal(e) =>
          log.warning("Failed to initialize. It will be retried. Caused by: {}", e.getMessage)
      }
  }

  override def postStop(): Unit = {
    if (sessionUsed)
      closeSession(cassandraSession.underlying)
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    mds <- Future(metadata(persistenceId, criteria).take(3).toVector)
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
    val stmt = cassandraSession.preparedSelectSnapshot.bind(metadata.persistenceId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(rs => deserialize(rs.one().getBytes("snapshot")))
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val stmt = cassandraSession.preparedWriteSnapshot.bind(metadata.persistenceId, metadata.sequenceNr: JLong,
      metadata.timestamp: JLong, serialize(Snapshot(snapshot)))
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val stmt = cassandraSession.preparedDeleteSnapshot.bind(metadata.persistenceId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = for {
    mds <- Future(metadata(persistenceId, criteria).toVector)
    res <- executeBatch(batch => mds.foreach(md => batch.add(
      cassandraSession.preparedDeleteSnapshot.bind(md.persistenceId, md.sequenceNr: JLong)
    )))
  } yield res

  def executeBatch(body: BatchStatement => Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def serialize(snapshot: Snapshot): ByteBuffer =
    ByteBuffer.wrap(serialization.findSerializerFor(snapshot).toBinary(snapshot))

  private def deserialize(bytes: ByteBuffer): Snapshot =
    serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get

  private def metadata(persistenceId: String, criteria: SnapshotSelectionCriteria): Iterator[SnapshotMetadata] =
    new RowIterator(persistenceId, criteria.maxSequenceNr).map { row =>
      SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp"))
    }.dropWhile(_.timestamp > criteria.maxTimestamp)

  private class RowIterator(persistenceId: String, maxSequenceNr: Long) extends Iterator[Row] {
    var currentSequenceNr = maxSequenceNr
    var rowCount = 0
    var iter = newIter()

    def newIter() = session.execute(selectSnapshotMetadata(Some(maxMetadataResultSize)), persistenceId, currentSequenceNr: JLong).iterator

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

}

private[snapshot] object CassandraSnapshotStore {
  private case object Init
}
