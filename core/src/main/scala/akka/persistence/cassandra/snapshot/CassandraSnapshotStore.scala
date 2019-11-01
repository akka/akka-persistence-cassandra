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
import com.datastax.driver.core.policies.LoggingRetryPolicy
import akka.persistence.cassandra.journal.FixedRetryPolicy
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import java.util.NoSuchElementException
import scala.util.Success
import scala.util.Failure

class CassandraSnapshotStore(
  cfg:     Config,
  cfgPath: String
) extends SnapshotStore with CassandraStatements with ActorLogging {
  import CassandraSnapshotStore._
  val config = new CassandraSnapshotStoreConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)
  val snapshotDeserializer = new SnapshotDeserializer(serialization)

  import config._
  import context.dispatcher

  private val someMaxLoadAttempts = Some(config.maxLoadAttempts)

  val session = new CassandraSession(
    context.system,
    config.sessionProvider,
    config.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = cfgPath,
    init = session => executeCreateKeyspaceAndTables(session, config)
  )

  private val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  private val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))
  private val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.readRetries))

  private def preparedWriteSnapshot = session.prepare(writeSnapshot(withMeta = false)).map(
    _.setConsistencyLevel(writeConsistency).setIdempotent(true).setRetryPolicy(writeRetryPolicy)
  )
  private def preparedWriteSnapshotWithMeta = session.prepare(writeSnapshot(withMeta = true)).map(
    _.setConsistencyLevel(writeConsistency).setIdempotent(true).setRetryPolicy(writeRetryPolicy)
  )
  private def preparedDeleteSnapshot = session.prepare(deleteSnapshot).map(
    _.setConsistencyLevel(writeConsistency).setIdempotent(true).setRetryPolicy(deleteRetryPolicy)
  )
  private def preparedSelectSnapshot = session.prepare(selectSnapshot).map(
    _.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy)
  )
  private def preparedSelectSnapshotMetadata =
    session.prepare(selectSnapshotMetadata(limit = None)).map(
      _.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy)
    )
  private def preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit =
    session.prepare(selectSnapshotMetadata(limit = Some(maxLoadAttempts))).map(
      _.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy)
    )

  private implicit val materializer = ActorMaterializer()

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraSnapshotStore.Init
  }

  override def receivePluginInternal: Receive = {
    case CassandraSnapshotStore.Init =>
      // try initialize early, to be prepared for first real request
      preparedWriteSnapshot
      preparedWriteSnapshotWithMeta
      preparedDeleteSnapshot
      preparedSelectSnapshot
      preparedSelectSnapshotMetadata
      preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit
  }

  override def postStop(): Unit = {
    session.close()
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    // The normal case is that timestamp is not specified (Long.MaxValue) in the criteria and then we can
    // use a select stmt with LIMIT if maxLoadAttempts, otherwise the result is iterated and
    // non-matching timestamps are discarded.
    val prepStmt =
      if (criteria.maxTimestamp == Long.MaxValue) preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit
      else preparedSelectSnapshotMetadata
    for {
      p <- prepStmt
      mds <- metadata(p, persistenceId, criteria, someMaxLoadAttempts)
      res <- loadNAsync(mds)
    } yield res
  }

  def loadNAsync(metadata: immutable.Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None) // no snapshots stored
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e: NoSuchElementException if metadata.size == 1 =>
        // Thrown load1Async when snapshot couldn't be found, which can happen since metadata and the
        // actual snapshot might not be replicated at exactly same time.
        // Treat this as if there were no snapshots.
        Future.successful(None)
      case e =>
        if (mds.isEmpty) {
          log.warning(
            s"Failed to load snapshot [$md] ({} of {}), last attempt. Caused by: [{}: {}]",
            maxLoadAttempts, maxLoadAttempts,
            e.getClass.getName, e.getMessage
          )
          Future.failed(e) // all attempts failed
        } else {
          log.warning(
            s"Failed to load snapshot [$md] ({} of {}), trying older one. Caused by: [{}: {}]",
            maxLoadAttempts - mds.size, maxLoadAttempts,
            e.getClass.getName, e.getMessage
          )
          loadNAsync(mds) // try older snapshot
        }
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val boundSelectSnapshot = preparedSelectSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundSelectSnapshot.flatMap(session.selectResultSet).map { rs =>
      val row = rs.one()
      if (row == null) {
        // Can happen since metadata and the actual snapshot might not be replicated at exactly same time.
        // Handled by loadNAsync.
        throw new NoSuchElementException(s"No snapshot for persistenceId [${metadata.persistenceId}] " +
          s"with with sequenceNr [${metadata.sequenceNr}]")
      } else {
        row.getBytes("snapshot") match {
          case null =>
            Snapshot(snapshotDeserializer.deserializeSnapshot(row))
          case bytes =>
            // for backwards compatibility
            serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get
        }
      }
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    Future(serialize(snapshot)).flatMap { ser =>

      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt = if (ser.meta.isDefined) preparedWriteSnapshotWithMeta else preparedWriteSnapshot

      stmt.flatMap { ps =>
        val bs = ps.bind()
        bs.setString("persistence_id", metadata.persistenceId)
        bs.setLong("sequence_nr", metadata.sequenceNr)
        bs.setLong("timestamp", metadata.timestamp)
        bs.setInt("ser_id", ser.serId)
        bs.setString("ser_manifest", ser.serManifest)
        bs.setBytes("snapshot_data", ser.serialized)

        // meta data, if any
        ser.meta match {
          case Some(meta) =>
            bs.setInt("meta_ser_id", meta.serId)
            bs.setString("meta_ser_manifest", meta.serManifest)
            bs.setBytes("meta", meta.serialized)
          case None =>
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
    preparedSelectSnapshotMetadata.flatMap { prepStmt =>
      metadata(prepStmt, persistenceId, criteria, limit = None).flatMap { mds =>
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
    def serializeMeta(): Option[SerializedMeta] = {
      // meta data, if any
      payload match {
        case SnapshotWithMetaData(_, m) =>
          val m2 = m.asInstanceOf[AnyRef]
          val serializer = serialization.findSerializerFor(m2)
          val serManifest = serializer match {
            case ser2: SerializerWithStringManifest =>
              ser2.manifest(m2)
            case _ =>
              if (serializer.includeManifest) m2.getClass.getName
              else PersistentRepr.Undefined
          }
          val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
          Some(SerializedMeta(metaBuf, serManifest, serializer.identifier))
        case evt => None
      }
    }

    def doSerializeSnapshot(): Serialized = {
      val p: AnyRef = (payload match {
        case SnapshotWithMetaData(snap, _) => snap // unwrap
        case snap                          => snap
      }).asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(p)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest =>
          ser2.manifest(p)
        case _ =>
          if (serializer.includeManifest) p.getClass.getName
          else PersistentRepr.Undefined
      }
      val serPayload = ByteBuffer.wrap(serialization.serialize(p).get)
      Serialized(serPayload, serManifest, serializer.identifier, serializeMeta())
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) => Serialization.currentTransportInformation.withValue(ti) { doSerializeSnapshot() }
      case None     => doSerializeSnapshot()
    }
  }

  private def metadata(prepStmt: PreparedStatement, persistenceId: String, criteria: SnapshotSelectionCriteria,
                       limit: Option[Int]): Future[immutable.Seq[SnapshotMetadata]] = {
    val boundStmt = prepStmt.bind(persistenceId, criteria.maxSequenceNr: JLong)
    val source = session.select(boundStmt)
      .map(row => SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp")))
      .dropWhile(_.timestamp > criteria.maxTimestamp)

    limit match {
      case Some(n) => source.take(n.toLong).runWith(Sink.seq)
      case None    => source.runWith(Sink.seq)
    }
  }

}

private[snapshot] object CassandraSnapshotStore {
  private case object Init

  private case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int,
                                meta: Option[SerializedMeta])

  private case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  class SnapshotDeserializer(serialization: Serialization) {

    // cache to avoid repeated check via ColumnDefinitions
    @volatile private var _hasMetaColumns: Option[Boolean] = None

    def hasMetaColumns(row: Row): Boolean = _hasMetaColumns match {
      case Some(b) => b
      case None =>
        val b = row.getColumnDefinitions.contains("meta")
        _hasMetaColumns = Some(b)
        b
    }

    def deserializeSnapshot(row: Row): Any = {
      val payload = serialization.deserialize(
        row.getBytes("snapshot_data").array,
        row.getInt("ser_id"),
        row.getString("ser_manifest")
      ).get

      if (hasMetaColumns(row)) {
        row.getBytes("meta") match {
          case null =>
            payload // no meta data
          case metaBytes =>
            // has meta data, wrap in EventWithMetaData
            val metaSerId = row.getInt("meta_ser_id")
            val metaSerManifest = row.getString("meta_ser_manifest")
            val meta = serialization.deserialize(
              metaBytes.array,
              metaSerId,
              metaSerManifest
            ) match {
              case Success(m) => m
              case Failure(_) =>
                // don't fail query because of deserialization problem with meta data
                // see motivation in UnknownMetaData
                SnapshotWithMetaData.UnknownMetaData(metaSerId, metaSerManifest)
            }
            SnapshotWithMetaData(payload, meta)
        }
      } else {
        // for backwards compatibility, when table was not altered, meta columns not added
        payload // no meta data
      }
    }
  }
}
