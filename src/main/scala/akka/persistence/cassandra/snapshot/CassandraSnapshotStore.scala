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
import akka.serialization.{ Serialization, SerializationExtension, SerializerWithStringManifest }
import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.typesafe.config.Config

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.collection.JavaConverters._

class CassandraSnapshotStore(cfg: Config) extends SnapshotStore with CassandraStatements with ActorLogging {
  val config = new CassandraSnapshotStoreConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)

  import CassandraSnapshotStore._
  import config._
  import context.dispatcher
  val blockingDispatcher = context.system.dispatchers.lookup(config.blockingDispatcherId)

  val session = new CassandraSession(context.system, config, context.dispatcher, log,
    metricsCategory = s"${self.path.name}",
    init = session => executeCreateKeyspaceAndTables(session, config))

  private def preparedWriteSnapshot = session.prepare(writeSnapshot).map(_.setConsistencyLevel(writeConsistency))
  private def preparedDeleteSnapshot = session.prepare(deleteSnapshot).map(_.setConsistencyLevel(writeConsistency))
  private def preparedSelectSnapshot = session.prepare(selectSnapshot).map(_.setConsistencyLevel(readConsistency))
  private def preparedSelectSnapshotMetadataForLoad =
    session.prepare(selectSnapshotMetadata(limit = Some(maxMetadataResultSize)))
      .map(_.setConsistencyLevel(readConsistency))
  private def preparedSelectSnapshotMetadataForDelete =
    session.prepare(selectSnapshotMetadata(limit = None)).map(_.setConsistencyLevel(readConsistency))

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
        log.warning("Failed to load snapshot, trying older one. Caused by: {}", e.getMessage)
        loadNAsync(mds) // try older snapshot
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val boundSelectSnapshot = preparedSelectSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundSelectSnapshot.flatMap(session.select).map { rs =>
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
        bs.setToNull("snapshot")
        session.executeWrite(bs)
      }
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val boundDeleteSnapshot = preparedDeleteSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundDeleteSnapshot.flatMap(session.executeWrite)
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
    session.underlying().flatMap(_.executeAsync(batch).asScala).map(_ => ())
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
    def iterate(nr: Long, buf: ListBuffer[SnapshotMetadata]): Future[Vector[SnapshotMetadata]] = {
      session.execute(prepStmt.bind(persistenceId, nr: JLong)) flatMap { rs =>
        var rowCount = 0
        var nextNr = 0l

        def fetchAvailable(rs: ResultSet): Future[Vector[SnapshotMetadata]] = {
          for (r <- rs.iterator.asScala.take(rs.getAvailableWithoutFetching)) {
            nextNr = r.getLong("sequence_nr")
            rowCount += 1
            buf += SnapshotMetadata(r.getString("persistence_id"), nextNr, r.getLong("timestamp"))
          }
          if (rowCount < maxMetadataResultSize) Future.successful(buf.dropWhile(_.timestamp > criteria.maxTimestamp).toVector)
          else if (rs.isFullyFetched) iterate(nextNr - 1, buf)
          else rs.fetchMoreResults.asScala.flatMap(fetchAvailable)
        }

        fetchAvailable(rs)
      }
    }
    iterate(criteria.maxSequenceNr, ListBuffer.empty)
  }

}

private[snapshot] object CassandraSnapshotStore {

  private case object Init

  private case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int)
}
