/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.typesafe.config.Config
import akka.persistence.cassandra.session.scaladsl.CassandraSession

class CassandraSnapshotStore(cfg: Config) extends SnapshotStore with CassandraStatements with ActorLogging {
  val config = new CassandraSnapshotStoreConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)

  import CassandraSnapshotStore._
  import config._
  import context.dispatcher
  val blockingDispatcher = context.system.dispatchers.lookup(config.blockingDispatcherId)

  val session = new CassandraSession(
    context.system,
    config.sessionProvider,
    config.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = s"${self.path.name}",
    init = session => executeCreateKeyspaceAndTables(session, config)
  )

  private def preparedWriteSnapshot = session.prepare(writeSnapshot).map(_.setConsistencyLevel(writeConsistency).setIdempotent(true))
  private def preparedDeleteSnapshot = session.prepare(deleteSnapshot).map(_.setConsistencyLevel(writeConsistency).setIdempotent(true))
  private def preparedSelectSnapshot = session.prepare(selectSnapshot).map(_.setConsistencyLevel(readConsistency).setIdempotent(true))
  private def preparedSelectSnapshotMetadataForLoad =
    session.prepare(selectSnapshotMetadata(limit = Some(maxMetadataResultSize)))
      .map(_.setConsistencyLevel(readConsistency).setIdempotent(true))
  private def preparedSelectSnapshotMetadataForDelete =
    session.prepare(selectSnapshotMetadata(limit = None)).map(_.setConsistencyLevel(readConsistency).setIdempotent(true))

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraSnapshotStore.Init
  }

  override def receivePluginInternal: Receive = {
    case CassandraSnapshotStore.Init =>
      // try initialize early, to be prepared for first real request
      preparedWriteSnapshot
      preparedDeleteSnapshot
      preparedSelectSnapshot
      preparedSelectSnapshotMetadataForLoad
      preparedSelectSnapshotMetadataForDelete
  }

  override def postStop(): Unit = {
    session.close()
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    prepStmt <- preparedSelectSnapshotMetadataForLoad
    mds <- metadata(prepStmt, persistenceId, criteria).map(_.take(3))
    res <- loadNAsync(mds)
  } yield res

  def loadNAsync(metadata: immutable.Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None)
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e =>
        log.warning("Failed to load snapshot, trying older one. Caused by: [{}: {}]", e.getClass.getName, e.getMessage)
        loadNAsync(mds) // try older snapshot
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val boundSelectSnapshot = preparedSelectSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundSelectSnapshot.flatMap(session.selectResultSet).map { rs =>
      val row = rs.one()
      row.getBytes("snapshot") match {
        case null =>
          Snapshot(serialization.deserialize(
            row.getBytes("snapshot_data").array,
            row.getInt("ser_id"),
            row.getString("ser_manifest")
          ).get)
        case bytes =>
          // for backwards compatibility
          serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get
      }
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    Future(serialize(snapshot)).flatMap { ser =>
      preparedWriteSnapshot.flatMap { ps =>
        val bs = ps.bind()
        bs.setString("persistence_id", metadata.persistenceId)
        bs.setLong("sequence_nr", metadata.sequenceNr)
        bs.setLong("timestamp", metadata.timestamp)
        bs.setInt("ser_id", ser.serId)
        bs.setString("ser_manifest", ser.serManifest)
        bs.setBytes("snapshot_data", ser.serialized)
        // for backwards compatibility
        if (session.protocolVersion.compareTo(ProtocolVersion.V4) < 0) {
          bs.setToNull("snapshot")
        } else {
          bs.unset("snapshot")
        }

        session.executeWrite(bs).map(_ => ())
      }
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val boundDeleteSnapshot = preparedDeleteSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundDeleteSnapshot.flatMap(session.executeWrite(_)).map(_ => ())
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    preparedSelectSnapshotMetadataForDelete.flatMap { prepStmt =>
      metadata(prepStmt, persistenceId, criteria).flatMap { mds =>
        val boundStatements = mds.map(md => preparedDeleteSnapshot.map(_.bind(md.persistenceId, md.sequenceNr: JLong)))
        Future.sequence(boundStatements).flatMap { stmts =>
          executeBatch(batch => stmts.foreach(batch.add))
        }
      }
    }
  }

  def executeBatch(body: BatchStatement => Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch)).map(_ => ())
  }

  private lazy val transportInformation: Option[Serialization.Information] = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, context.system))
  }

  private def serialize(payload: Any): Serialized = {
    def doSerializeSnapshot(): Serialized = {
      val p: AnyRef = payload.asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(p)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest ⇒
          ser2.manifest(p)
        case _ ⇒
          if (serializer.includeManifest) p.getClass.getName
          else PersistentRepr.Undefined
      }
      val serPayload = ByteBuffer.wrap(serialization.serialize(p).get)
      Serialized(serPayload, serManifest, serializer.identifier)
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) ⇒ Serialization.currentTransportInformation.withValue(ti) { doSerializeSnapshot() }
      case None     ⇒ doSerializeSnapshot()
    }
  }

  private def metadata(prepStmt: PreparedStatement, persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Vector[SnapshotMetadata]] = {
    // TODO the RowIterator is using some blocking, would benefit from a rewrite
    Future {
      new RowIterator(prepStmt, persistenceId, criteria.maxSequenceNr).map { row =>
        SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp"))
      }.dropWhile(_.timestamp > criteria.maxTimestamp).toVector
    }(blockingDispatcher)
  }

  private class RowIterator(prepStmt: PreparedStatement, persistenceId: String, maxSequenceNr: Long) extends Iterator[Row] {
    var currentSequenceNr = maxSequenceNr
    var rowCount = 0

    // we know that the session is initialized, since we got the prepStmt
    val ses = Await.result(session.underlying(), config.blockingTimeout)

    // FIXME more blocking
    var iter = newIter()
    def newIter() = ses.execute(prepStmt.bind(persistenceId, currentSequenceNr: JLong)).iterator

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

  private case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int)
}
