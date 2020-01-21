/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.NoSuchElementException

import akka.NotUsed
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra._
import akka.cassandra.session.scaladsl.CassandraSession
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.AsyncSerializer
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.protocol.internal.util.Bytes
import com.typesafe.config.Config

class CassandraSnapshotStore(cfg: Config, cfgPath: String)
    extends SnapshotStore
    with CassandraStatements
    with ActorLogging
    with CassandraSnapshotCleanup {

  import CassandraSnapshotStore._

  // shared config is one level above the journal specific
  private val sharedConfigPath = cfgPath.replaceAll("""\.snapshot""", "")
  private val sharedConfig = context.system.settings.config.getConfig(sharedConfigPath)
  override val settings = new PluginSettings(context.system, sharedConfig)
  override def snapshotSettings = settings.snapshotSettings
  val serialization = SerializationExtension(context.system)
  val snapshotDeserializer = new SnapshotDeserializer(context.system)
  implicit val ec: ExecutionContext = context.dispatcher

  private val someMaxLoadAttempts = Some(snapshotSettings.maxLoadAttempts)

  val session: CassandraSession = CassandraSessionRegistry(context.system)
    .sessionFor(sharedConfigPath, context.dispatcher, ses => executeAllCreateKeyspaceAndTables(ses))

  private def preparedWriteSnapshot =
    session.prepare(writeSnapshot(withMeta = false))
  private def preparedWriteSnapshotWithMeta =
    session.prepare(writeSnapshot(withMeta = true))
  private def preparedSelectSnapshot =
    session.prepare(selectSnapshot)
  private def preparedSelectSnapshotMetadata: Future[PreparedStatement] =
    session.prepare(selectSnapshotMetadata(limit = None))
  private def preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit: Future[PreparedStatement] =
    session.prepare(selectSnapshotMetadata(limit = Some(snapshotSettings.maxLoadAttempts)))

  private implicit val materializer = ActorMaterializer()

  override def preStart(): Unit =
    // eager initialization, but not from constructor
    self ! CassandraSnapshotStore.Init

  override def receivePluginInternal: Receive = {
    case CassandraSnapshotStore.Init =>
      log.debug("Initializing")
      // try initialize early, to be prepared for first real request
      preparedWriteSnapshot
      preparedWriteSnapshotWithMeta
      preparedDeleteSnapshot
      preparedDeleteAllSnapshotsForPid
      if (!settings.cassandra2xCompat) preparedDeleteAllSnapshotsForPidAndSequenceNrBetween
      preparedSelectSnapshot
      preparedSelectSnapshotMetadata
      preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit
      log.debug("Initialized")
  }

  override def postStop(): Unit =
    session.close()

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("loadAsync [{}] [{}]", persistenceId, criteria)
    // The normal case is that timestamp is not specified (Long.MaxValue) in the criteria and then we can
    // use a select stmt with LIMIT if maxLoadAttempts, otherwise the result is iterated and
    // non-matching timestamps are discarded.
    val snapshotMetaPs =
      if (criteria.maxTimestamp == Long.MaxValue)
        preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit
      else preparedSelectSnapshotMetadata

    for {
      p <- snapshotMetaPs
      mds <- metadata(p, persistenceId, criteria, someMaxLoadAttempts)
      res <- loadNAsync(mds)
    } yield res
  }

  private def loadNAsync(metadata: immutable.Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None) // no snapshots stored
    case md +: mds =>
      load1Async(md)
        .map {
          case Snapshot(s) => Some(SelectedSnapshot(md, s))
        }
        .recoverWith {
          case _: NoSuchElementException if metadata.size == 1 =>
            // Thrown load1Async when snapshot couldn't be found, which can happen since metadata and the
            // actual snapshot might not be replicated at exactly same time.
            // Treat this as if there were no snapshots.
            Future.successful(None)
          case e =>
            if (mds.isEmpty) {
              log.warning(
                s"Failed to load snapshot [$md] ({} of {}), last attempt. Caused by: [{}: {}]",
                snapshotSettings.maxLoadAttempts,
                snapshotSettings.maxLoadAttempts,
                e.getClass.getName,
                e.getMessage)
              Future.failed(e) // all attempts failed
            } else {
              log.warning(
                s"Failed to load snapshot [$md] ({} of {}), trying older one. Caused by: [{}: {}]",
                snapshotSettings.maxLoadAttempts - mds.size,
                snapshotSettings.maxLoadAttempts,
                e.getClass.getName,
                e.getMessage)
              loadNAsync(mds) // try older snapshot
            }
        }
  }

  private def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val boundSelectSnapshot = preparedSelectSnapshot.map(
      _.bind(metadata.persistenceId, metadata.sequenceNr: JLong).setExecutionProfileName(snapshotSettings.readProfile))
    boundSelectSnapshot.flatMap(session.selectOne).flatMap {
      case None =>
        // Can happen since metadata and the actual snapshot might not be replicated at exactly same time.
        // Handled by loadNAsync.
        throw new NoSuchElementException(
          s"No snapshot for persistenceId [${metadata.persistenceId}] " +
          s"with with sequenceNr [${metadata.sequenceNr}]")
      case Some(row) =>
        row.getByteBuffer("snapshot") match {
          case null =>
            snapshotDeserializer.deserializeSnapshot(row).map(Snapshot.apply)
          case bytes =>
            // for backwards compatibility
            Future.successful(serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get)
        }
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    serialize(snapshot).flatMap { ser =>
      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt =
        if (ser.meta.isDefined) preparedWriteSnapshotWithMeta
        else preparedWriteSnapshot

      stmt.flatMap { ps =>
        val bs = ps
          .bind()
          .setString("persistence_id", metadata.persistenceId)
          .setLong("sequence_nr", metadata.sequenceNr)
          .setLong("timestamp", metadata.timestamp)
          .setInt("ser_id", ser.serId)
          .setString("ser_manifest", ser.serManifest)
          .setByteBuffer("snapshot_data", ser.serialized)

        // meta data, if any
        val finished = ser.meta match {
          case Some(meta) =>
            bs.setInt("meta_ser_id", meta.serId)
              .setString("meta_ser_manifest", meta.serManifest)
              .setByteBuffer("meta", meta.serialized)
          case None =>
            bs
        }

        session.executeWrite(finished.setExecutionProfileName(snapshotSettings.writeProfile)).map(_ => ())
      }
    }

  /** Plugin API: deletes all snapshots matching `criteria`. This call is protected with a circuit-breaker.
   *
   * @param persistenceId id of the persistent actor.
   * @param criteria selection criteria for deleting. If no timestamp constraints are specified this routine
   * @note Due to the limitations of Cassandra deletion requests, this routine makes an initial query in order to obtain the
   * records matching the criteria which are then deleted in a batch deletion. Improvements in Cassandra v3.0+ mean a single
   * range deletion on the sequence number is used instead, except if timestamp constraints are specified, which still
   * requires the original two step routine.*/
  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    if (settings.cassandra2xCompat
        || 0L < criteria.minTimestamp
        || criteria.maxTimestamp < SnapshotSelectionCriteria.latest().maxTimestamp) {
      preparedSelectSnapshotMetadata.flatMap { snapshotMetaPs =>
        // this meta query gets slower than slower if snapshots are deleted without a criteria.minSequenceNr as
        // all previous tombstones are scanned in the meta data query
        metadata(snapshotMetaPs, persistenceId, criteria, limit = None).flatMap {
          mds: immutable.Seq[SnapshotMetadata] =>
            val boundStatementBatches = mds
              .map(md => preparedDeleteSnapshot.map(_.bind(md.persistenceId, md.sequenceNr: JLong)))
              .grouped(0xFFFF - 1)
            if (boundStatementBatches.nonEmpty) {
              Future
                .sequence(
                  boundStatementBatches.map(
                    boundStatements =>
                      Future
                        .sequence(boundStatements)
                        .flatMap(stmts => executeBatch(batch => stmts.foreach(batch.addStatement)))))
                .map(_ => ())
            } else {
              Future.successful(())
            }
        }
      }
    } else {
      val boundDeleteSnapshot = preparedDeleteAllSnapshotsForPidAndSequenceNrBetween.map(
        _.bind(persistenceId, criteria.minSequenceNr: JLong, criteria.maxSequenceNr: JLong))
      boundDeleteSnapshot.flatMap(session.executeWrite(_)).map(_ => ())
    }
  }

  def executeBatch(body: BatchStatementBuilder => Unit): Future[Unit] = {
    import scala.compat.java8.FutureConverters._
    val batch =
      new BatchStatementBuilder(BatchType.UNLOGGED).setExecutionProfileName(snapshotSettings.writeProfile)
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch.build()).toScala).map(_ => ())
  }

  private def serialize(payload: Any): Future[Serialized] =
    try {
      def serializeMeta(): Option[SerializedMeta] =
        // meta data, if any
        payload match {
          case SnapshotWithMetaData(_, m) =>
            val m2 = m.asInstanceOf[AnyRef]
            val serializer = serialization.findSerializerFor(m2)
            val serManifest = Serializers.manifestFor(serializer, m2)
            val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
            Some(SerializedMeta(metaBuf, serManifest, serializer.identifier))
          case _ => None
        }

      val p: AnyRef = (payload match {
        case SnapshotWithMetaData(snap, _) => snap // unwrap
        case snap                          => snap
      }).asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(p)
      val serManifest = Serializers.manifestFor(serializer, p)
      serializer match {
        case asyncSer: AsyncSerializer =>
          Serialization.withTransportInformation(context.system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSer.toBinaryAsync(p).map { bytes =>
              val serPayload = ByteBuffer.wrap(bytes)
              Serialized(serPayload, serManifest, serializer.identifier, serializeMeta())
            }
          }
        case _ =>
          Future {
            // Serialization.serialize adds transport info
            val serPayload = ByteBuffer.wrap(serialization.serialize(p).get)
            Serialized(serPayload, serManifest, serializer.identifier, serializeMeta())
          }
      }

    } catch {
      case NonFatal(e) => Future.failed(e)
    }

  private def metadata(
      snapshotMetaPs: PreparedStatement,
      persistenceId: String,
      criteria: SnapshotSelectionCriteria,
      limit: Option[Int]): Future[immutable.Seq[SnapshotMetadata]] = {
    val boundStmt = snapshotMetaPs.bind(persistenceId, criteria.maxSequenceNr: JLong, criteria.minSequenceNr: JLong)
    log.debug("Executing metadata query")
    val source: Source[SnapshotMetadata, NotUsed] = session
      .select(boundStmt)
      .map(row =>
        SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp")))
      .dropWhile(_.timestamp > criteria.maxTimestamp)

    limit match {
      case Some(n) => source.take(n.toLong).runWith(Sink.seq)
      case None    => source.runWith(Sink.seq)
    }
  }

}

private[snapshot] object CassandraSnapshotStore {
  private case object Init

  private case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int, meta: Option[SerializedMeta])

  private case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  class SnapshotDeserializer(system: ActorSystem) {

    private val serialization = SerializationExtension(system)

    // cache to avoid repeated check via ColumnDefinitions
    @volatile private var _hasMetaColumns: Option[Boolean] = None

    def hasMetaColumns(row: Row): Boolean = _hasMetaColumns match {
      case Some(b) => b
      case None =>
        val b = row.getColumnDefinitions.contains("meta")
        _hasMetaColumns = Some(b)
        b
    }

    def deserializeSnapshot(row: Row)(implicit ec: ExecutionContext): Future[Any] =
      try {

        def meta: OptionVal[AnyRef] =
          if (hasMetaColumns(row)) {
            row.getByteBuffer("meta") match {
              case null =>
                OptionVal.None // no meta data
              case metaBytes =>
                // has meta data, wrap in EventWithMetaData
                val metaSerId = row.getInt("meta_ser_id")
                val metaSerManifest = row.getString("meta_ser_manifest")
                val meta = serialization.deserialize(Bytes.getArray(metaBytes), metaSerId, metaSerManifest) match {
                  case Success(m) => m
                  case Failure(_) =>
                    // don't fail query because of deserialization problem with meta data
                    // see motivation in UnknownMetaData
                    SnapshotWithMetaData.UnknownMetaData(metaSerId, metaSerManifest)
                }
                OptionVal.Some(meta)
            }
          } else {
            // for backwards compatibility, when table was not altered, meta columns not added
            OptionVal.None // no meta data
          }

        val bytes = Bytes.getArray(row.getByteBuffer("snapshot_data"))
        val serId = row.getInt("ser_id")
        val manifest = row.getString("ser_manifest")
        serialization.serializerByIdentity.get(serId) match {
          case Some(asyncSerializer: AsyncSerializer) =>
            Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
              asyncSerializer.fromBinaryAsync(bytes, manifest).map { payload =>
                meta match {
                  case OptionVal.None    => payload
                  case OptionVal.Some(m) => SnapshotWithMetaData(payload, m)
                }
              }
            }

          case _ =>
            Future.successful {
              // Serialization.deserialize adds transport info
              val payload =
                serialization.deserialize(bytes, serId, manifest).get
              meta match {
                case OptionVal.None    => payload
                case OptionVal.Some(m) => SnapshotWithMetaData(payload, m)
              }
            }
        }

      } catch {
        case NonFatal(e) => Future.failed(e)
      }
  }
}
