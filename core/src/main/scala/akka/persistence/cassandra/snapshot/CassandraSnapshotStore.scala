/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.NoSuchElementException

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.Done
import akka.actor._
import akka.annotation.InternalApi
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.FixedRetryPolicy
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.AsyncSerializer
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.ActorAttributes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.datastax.driver.core._
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.utils.Bytes
import akka.util.OptionVal
import com.typesafe.config.Config

class CassandraSnapshotStore(cfg: Config)
    extends SnapshotStore
    with CassandraStatements
    with ActorLogging
    with CassandraSnapshotCleanup {

  import CassandraSnapshotStore._

  val snapshotConfig = new CassandraSnapshotStoreConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)
  val snapshotSerialization = new SnapshotSerialization(context.system)
  implicit val ec: ExecutionContext = context.dispatcher

  import snapshotConfig._

  private val someMaxLoadAttempts = Some(snapshotConfig.maxLoadAttempts)

  val session = new CassandraSession(
    context.system,
    snapshotConfig.sessionProvider,
    snapshotConfig.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = s"${self.path.name}",
    init = session => executeCreateKeyspaceAndTables(session, snapshotConfig))

  private val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(snapshotConfig.writeRetries))
  private val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(snapshotConfig.readRetries))

  private def preparedWriteSnapshot =
    session
      .prepare(writeSnapshot(withMeta = false))
      .map(_.setConsistencyLevel(writeConsistency).setIdempotent(true).setRetryPolicy(writeRetryPolicy))
  private def preparedWriteSnapshotWithMeta =
    session
      .prepare(writeSnapshot(withMeta = true))
      .map(_.setConsistencyLevel(writeConsistency).setIdempotent(true).setRetryPolicy(writeRetryPolicy))

  private def preparedSelectSnapshot =
    session
      .prepare(selectSnapshot)
      .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  private def preparedSelectSnapshotMetadata: Future[PreparedStatement] =
    session
      .prepare(selectSnapshotMetadata(limit = None))
      .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  private def preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit: Future[PreparedStatement] =
    session
      .prepare(selectSnapshotMetadata(limit = Some(maxLoadAttempts)))
      .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))

  private implicit val materializer = ActorMaterializer()

  override def preStart(): Unit =
    // eager initialization, but not from constructor
    self ! CassandraSnapshotStore.Init

  override def receivePluginInternal: Receive = {
    case CassandraSnapshotStore.Init =>
      // try initialize early, to be prepared for first real request
      preparedWriteSnapshot
      preparedWriteSnapshotWithMeta
      preparedDeleteSnapshot
      preparedDeleteAllSnapshotsForPid
      if (!snapshotConfig.cassandra2xCompat) preparedDeleteAllSnapshotsForPidAndSequenceNrBetween
      preparedSelectSnapshot
      preparedSelectSnapshotMetadata
      preparedSelectSnapshotMetadataWithMaxLoadAttemptsLimit

    case DeleteAllSnapshots(persistenceId) =>
      val result: Future[Done] =
        deleteAsync(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = Long.MaxValue)).map(_ => Done)
      result.pipeTo(sender())
  }

  override def postStop(): Unit =
    session.close()

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
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
                maxLoadAttempts,
                maxLoadAttempts,
                e.getClass.getName,
                e.getMessage)
              Future.failed(e) // all attempts failed
            } else {
              log.warning(
                s"Failed to load snapshot [$md] ({} of {}), trying older one. Caused by: [{}: {}]",
                maxLoadAttempts - mds.size,
                maxLoadAttempts,
                e.getClass.getName,
                e.getMessage)
              loadNAsync(mds) // try older snapshot
            }
        }
  }

  private def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val boundSelectSnapshot = preparedSelectSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundSelectSnapshot.flatMap(session.selectOne).flatMap {
      case None =>
        // Can happen since metadata and the actual snapshot might not be replicated at exactly same time.
        // Handled by loadNAsync.
        throw new NoSuchElementException(
          s"No snapshot for persistenceId [${metadata.persistenceId}] " +
          s"with with sequenceNr [${metadata.sequenceNr}]")
      case Some(row) =>
        row.getBytes("snapshot") match {
          case null =>
            snapshotSerialization.deserializeSnapshot(row).map(Snapshot.apply)
          case bytes =>
            // for backwards compatibility
            Future.successful(serialization.deserialize(Bytes.getArray(bytes), classOf[Snapshot]).get)
        }
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    snapshotSerialization.serialize(snapshot).flatMap { ser =>
      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt =
        if (ser.meta.isDefined) preparedWriteSnapshotWithMeta
        else preparedWriteSnapshot

      stmt.flatMap { ps =>
        val bs = CassandraSnapshotStore.prepareSnapshotWrite(ps, metadata, ser)
        session.executeWrite(bs).map(_ => ())
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
    if (snapshotConfig.cassandra2xCompat
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
                .sequence(boundStatementBatches.map(boundStatements =>
                  Future.sequence(boundStatements).flatMap(stmts => executeBatch(batch => stmts.foreach(batch.add)))))
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

  def executeBatch(body: BatchStatement => Unit): Future[Unit] = {
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      .setConsistencyLevel(writeConsistency)
      .asInstanceOf[BatchStatement]
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch)).map(_ => ())
  }

  private def metadata(
      snapshotMetaPs: PreparedStatement,
      persistenceId: String,
      criteria: SnapshotSelectionCriteria,
      limit: Option[Int]): Future[immutable.Seq[SnapshotMetadata]] = {
    val boundStmt = snapshotMetaPs.bind(persistenceId, criteria.maxSequenceNr: JLong, criteria.minSequenceNr: JLong)
    val source = session
      .select(boundStmt)
      .map(row =>
        SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp")))
      .dropWhile(_.timestamp > criteria.maxTimestamp)

    val limitedSource = limit match {
      case Some(n) => source.take(n.toLong)
      case None    => source
    }
    limitedSource
      .toMat(Sink.seq)(Keep.right)
      .withAttributes(ActorAttributes.dispatcher(snapshotConfig.sessionSettings.pluginDispatcher))
      .run()
  }

}

@InternalApi private[akka] object CassandraSnapshotStore {
  private case object Init

  sealed trait CleanupCommand
  final case class DeleteAllSnapshots(persistenceId: String) extends CleanupCommand

  final case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int, meta: Option[SerializedMeta])

  final case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] class SnapshotSerialization(system: ActorSystem) {

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

    def serialize(payload: Any)(implicit ec: ExecutionContext): Future[Serialized] =
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
            Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
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

    def deserializeSnapshot(row: Row)(implicit ec: ExecutionContext): Future[Any] =
      try {

        def meta: OptionVal[AnyRef] =
          if (hasMetaColumns(row)) {
            row.getBytes("meta") match {
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

        val bytes = Bytes.getArray(row.getBytes("snapshot_data"))
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

  /**
   * INTERNAL API
   */
  private[akka] def prepareSnapshotWrite(
      ps: PreparedStatement,
      metadata: SnapshotMetadata,
      ser: Serialized): BoundStatement = {
    val bs = ps
      .bind()
      .setString("persistence_id", metadata.persistenceId)
      .setLong("sequence_nr", metadata.sequenceNr)
      .setLong("timestamp", metadata.timestamp)
      .setInt("ser_id", ser.serId)
      .setString("ser_manifest", ser.serManifest)
      .setBytes("snapshot_data", ser.serialized)

    // meta data, if any
    ser.meta match {
      case Some(meta) =>
        bs.setInt("meta_ser_id", meta.serId)
          .setString("meta_ser_manifest", meta.serManifest)
          .setBytes("meta", meta.serialized)
      case None =>
        bs
    }
  }
}
