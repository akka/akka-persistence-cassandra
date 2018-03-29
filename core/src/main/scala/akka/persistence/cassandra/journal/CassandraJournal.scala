/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.{ UUID, HashMap => JHMap, Map => JMap }

import akka.Done
import akka.actor.{ ActorRef, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence._
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TagWriters.{ BulkTagWrite, TagWrite, TagWritersSession }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.query.PersistenceQuery
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.utils.{ Bytes, UUIDs }
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util.{ Failure, Success, Try }

class CassandraJournal(cfg: Config) extends AsyncWriteJournal
  with CassandraRecovery
  with CassandraStatements
  with NoSerializationVerificationNeeded {

  private[akka] val config = new CassandraJournalConfig(context.system, cfg)
  private[akka] val serialization = SerializationExtension(context.system)
  private[akka] val log: LoggingAdapter = Logging(context.system, getClass)

  import CassandraJournal._
  import config._

  implicit override val ec: ExecutionContext = context.dispatcher

  // readHighestSequence must be performed after pending write for a persistenceId
  // when the persistent actor is restarted.
  // It seems like C* doesn't support session consistency so we handle it ourselves.
  // https://aphyr.com/posts/299-the-trouble-with-timestamps
  private val writeInProgress: JMap[String, Future[Done]] = new JHMap

  val session = new CassandraSession(
    context.system,
    config.sessionProvider,
    config.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = s"${self.path.name}",
    init = session =>
    executeCreateKeyspaceAndTables(session, config)
  )

  private val tagWriterSession = TagWritersSession(
    preparedWriteToTagViewWithoutMeta,
    preparedWriteToTagViewWithMeta,
    session.executeWrite,
    session.selectResultSet,
    preparedWriteToTagProgress,
    preparedWriteTagScanning
  )

  protected val tagWrites: Option[ActorRef] =
    if (config.eventsByTagEnabled)
      Some(context.actorOf(TagWriters.props(config.tagWriterSettings, tagWriterSession)
        .withDispatcher(context.props.dispatcher), "tagWrites"))
    else None

  def preparedWriteMessage = session.prepare(writeMessage(withMeta = false)).map(_.setIdempotent(true))

  def preparedWriteMessageWithMeta = session.prepare(
    writeMessage(withMeta = true)
  ).map(_.setIdempotent(true))
  def preparedSelectMessages = session.prepare(selectMessages)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedWriteInUse = session.prepare(writeInUse).map(_.setIdempotent(true))

  override implicit val materializer: ActorMaterializer = ActorMaterializer()(context.system)

  private[akka] lazy val queries =
    PersistenceQuery(context.system.asInstanceOf[ExtendedActorSystem])
      .readJournalFor[CassandraReadJournal](config.queryPlugin)

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraJournal.Init
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)

    case CassandraJournal.Init =>
      // try initialize early, to be prepared for first real request
      preparedWriteMessage
      preparedWriteMessageWithMeta
      preparedDeleteMessages
      preparedSelectMessages
      preparedWriteInUse
      preparedSelectHighestSequenceNr
      preparedSelectDeletedTo
      preparedInsertDeletedTo

      if (config.eventsByTagEnabled) {
        preparedSelectTagProgress
        preparedSelectTagProgressForPersistenceId
        preparedWriteToTagProgress
        preparedWriteToTagViewWithoutMeta
        preparedWriteToTagViewWithMeta
        preparedWriteTagScanning
      }
  }

  private[akka] val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  private[akka] val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.readRetries))
  private[akka] val someReadRetryPolicy = Some(readRetryPolicy)
  private[akka] val someReadConsistency = Some(config.readConsistency)

  override def postStop(): Unit = {
    session.close()
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    //
    // We must NOT catch serialization exceptions here because rejections will cause
    // holes in the sequence number series and we use the sequence numbers to detect
    // missing (delayed) events in the eventByTag query.
    //
    // Note that we assume that all messages have the same persistenceId, which is
    // the case for Akka 2.4.2.

    def serialize(aw: Seq[(PersistentRepr, UUID)]): SerializedAtomicWrite = {

      SerializedAtomicWrite(
        aw.head._1.persistenceId,
        aw.map {
          case (pr, uuid) =>
            val (pr2, tags) = pr.payload match {
              case Tagged(payload, ts) =>
                (pr.withPayload(payload), ts)
              case _ =>
                (pr, Set.empty[String])
            }
            serializeEvent(pr2, tags, uuid, config.bucketSize, serialization, transportInformation)
        }
      )
    }

    val writesWithUuids: Seq[Seq[(PersistentRepr, UUID)]] = messages
      .map(aw => aw.payload.map(pr => (pr, UUIDs.timeBased())))

    val p = Promise[Done]
    val pid = messages.head.persistenceId
    writeInProgress.put(pid, p.future)

    val tws = Future(writesWithUuids.map(serialize)).flatMap { serialized: Seq[SerializedAtomicWrite] =>
      val result: Future[Any] =
        if (messages.size <= config.maxMessageBatchSize) {
          // optimize for the common case
          writeMessages(serialized)
        } else {
          val groups: List[Seq[SerializedAtomicWrite]] = serialized.grouped(config.maxMessageBatchSize).toList

          // execute the groups in sequence
          def rec(todo: List[Seq[SerializedAtomicWrite]], acc: List[Unit]): Future[List[Unit]] =
            todo match {
              case write :: remainder => writeMessages(write).flatMap(result => rec(remainder, result :: acc))
              case Nil                => Future.successful(acc.reverse)
            }

          rec(groups, Nil)
        }
      result.map(_ => extractTagWrites(serialized))
    }

    tws.onComplete { result =>
      self ! WriteFinished(pid, p.future)
      p.success(Done)
      // notify TagWriters when write was successful
      result.foreach(bulkTagWrite => tagWrites.foreach(_ ! bulkTagWrite))
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

    //Nil == all good
    tws.map(_ => Nil)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
  }

  private def extractTagWrites(serialized: Seq[SerializedAtomicWrite]): BulkTagWrite = {
    if (serialized.isEmpty) BulkTagWrite(Nil, Nil)
    else if (serialized.size == 1 && serialized.head.payload.size == 1) {
      // optimization for one single event, which is the typical case
      val s = serialized.head.payload.head
      if (s.tags.isEmpty) BulkTagWrite(Nil, s :: Nil)
      else BulkTagWrite(s.tags.map(tag => TagWrite(tag, s :: Nil))(collection.breakOut), Nil)
    } else {
      val messagesByTag: Map[String, Seq[Serialized]] = serialized
        .flatMap(_.payload)
        .flatMap(s => s.tags.map((_, s)))
        .groupBy(_._1).mapValues(_.map(_._2))
      val messagesWithoutTag =
        for {
          a <- serialized
          b <- a.payload
          if b.tags.isEmpty
        } yield b

      val writesWithTags: immutable.Seq[TagWrite] = messagesByTag.map {
        case (tag, writes) => TagWrite(tag, writes)
      }(collection.breakOut)

      BulkTagWrite(writesWithTags, messagesWithoutTag)
    }

  }

  private def writeMessages(atomicWrites: Seq[SerializedAtomicWrite]): Future[Unit] = {
    val boundStatements = statementGroup(atomicWrites)
    boundStatements.size match {
      case 1 =>
        boundStatements.head.flatMap(execute(_, writeRetryPolicy))
      case 0 => Future.successful(())
      case _ =>
        Future.sequence(boundStatements).flatMap { stmts =>
          executeBatch(batch => stmts.foreach(batch.add), writeRetryPolicy)
        }
    }
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[Future[BoundStatement]] = {
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr, targetPartitionSize)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq, targetPartitionSize)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[Future[BoundStatement]] = all.map { m: Serialized =>
      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt = if (m.meta.isDefined) preparedWriteMessageWithMeta else preparedWriteMessage

      stmt.map {
        stmt =>
          val bs = stmt.bind()
          bs.setString("persistence_id", persistenceId)
          bs.setLong("partition_nr", maxPnr)
          bs.setLong("sequence_nr", m.sequenceNr)
          bs.setUUID("timestamp", m.timeUuid)
          // Keeping as text for backward compatibility
          bs.setString("timebucket", m.timeBucket.key.toString)
          bs.setString("writer_uuid", m.writerUuid)
          bs.setInt("ser_id", m.serId)
          bs.setString("ser_manifest", m.serManifest)
          bs.setString("event_manifest", m.eventAdapterManifest)
          bs.setBytes("event", m.serialized)
          bs.setSet("tags", m.tags.asJava, classOf[String])

          // meta data, if any
          m.meta.foreach(meta => {
            bs.setInt("meta_ser_id", meta.serId)
            bs.setString("meta_ser_manifest", meta.serManifest)
            bs.setBytes("meta", meta.serialized)
          })
          bs
      }
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr)
      writes :+ preparedWriteInUse.map(_.bind(persistenceId, minPnr: JLong))
    else
      writes

  }

  /**
   * It is assumed that this is only called during a replay and if fromSequenceNr == highest
   * then asyncReplayMessages won't be called. In that case the tag progress is updated
   * in here rather than during replay messages.
   */
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val highestSequenceNr = writeInProgress.get(persistenceId) match {
      case null => asyncReadHighestSequenceNrInternal(persistenceId, fromSequenceNr)
      case f    => f.flatMap(_ => asyncReadHighestSequenceNrInternal(persistenceId, fromSequenceNr))
    }

    if (config.eventsByTagEnabled) {
      // map to send tag write progress so actor doesn't finish recovery until it is done
      highestSequenceNr.flatMap { seqNr =>
        if (seqNr == fromSequenceNr && seqNr != 0) {
          log.debug("Snapshot is current so replay won't be required. Calculating tag progress now.")
          val scanningSeqNrFut = tagScanningStartingSequenceNr(persistenceId)
          for {
            tp <- lookupTagProgress(persistenceId)
            _ <- sendTagProgress(persistenceId, tp, tagWrites.get)
            scanningSeqNr <- scanningSeqNrFut
            _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, Long.MaxValue, tp)
          } yield seqNr
        } else {
          Future.successful(seqNr)
        }
      }
    } else {
      highestSequenceNr
    }
  }

  private def executeBatch(body: BatchStatement ⇒ Unit, retryPolicy: RetryPolicy): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy).asInstanceOf[BatchStatement]
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch)).map(_ => ())
  }

  private def execute(stmt: Statement, retryPolicy: RetryPolicy): Future[Unit] = {
    stmt.setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy)
    session.executeWrite(stmt).map(_ => ())
  }

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  protected lazy val transportInformation: Option[Serialization.Information] = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, context.system))
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraJournal {
  private[akka]type Tag = String
  private[akka]type PersistenceId = String
  private[akka]type SequenceNr = Long
  private[akka]type TagPidSequenceNr = Long

  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])

  private[akka] case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                      eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
                                      meta: Option[SerializedMeta], timeUuid: UUID, timeBucket: TimeBucket)

  private[akka] case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  class EventDeserializer(serialization: Serialization) {

    // caching to avoid repeated check via ColumnDefinitions
    private def hasColumn(column: String, row: Row, cached: Option[Boolean], updateCache: Boolean => Unit): Boolean = {
      cached match {
        case Some(b) => b
        case None =>
          val b = row.getColumnDefinitions.contains(column)
          updateCache(b)
          b
      }
    }

    @volatile private var _hasMetaColumns: Option[Boolean] = None
    private val updateMetaColumnsCache: Boolean => Unit = b => _hasMetaColumns = Some(b)
    def hasMetaColumns(row: Row): Boolean =
      hasColumn("meta", row, _hasMetaColumns, updateMetaColumnsCache)

    @volatile private var _hasOldTagsColumns: Option[Boolean] = None
    private val updateOldTagsColumnsCache: Boolean => Unit = b => _hasOldTagsColumns = Some(b)
    def hasOldTagsColumns(row: Row): Boolean =
      hasColumn("tag1", row, _hasOldTagsColumns, updateOldTagsColumnsCache)

    @volatile private var _hasTagsColumn: Option[Boolean] = None
    private val updateTagsColumnCache: Boolean => Unit = b => _hasTagsColumn = Some(b)
    def hasTagsColumn(row: Row): Boolean =
      hasColumn("tags", row, _hasTagsColumn, updateTagsColumnCache)

    def deserializeEvent(row: Row): Any = {
      val event = serialization.deserialize(
        Bytes.getArray(row.getBytes("event")),
        row.getInt("ser_id"),
        row.getString("ser_manifest")
      ).get

      if (hasMetaColumns(row)) {
        row.getBytes("meta") match {
          case null =>
            event // no meta data
          case metaBytes =>
            // has meta data, wrap in EventWithMetaData
            val metaSerId = row.getInt("meta_ser_id")
            val metaSerManifest = row.getString("meta_ser_manifest")
            val meta = serialization.deserialize(
              Bytes.getArray(metaBytes),
              metaSerId,
              metaSerManifest
            ) match {
                case Success(m) => m
                case Failure(_) =>
                  // don't fail replay/query because of deserialization problem with meta data
                  // see motivation in UnknownMetaData
                  UnknownMetaData(metaSerId, metaSerManifest)
              }
            EventWithMetaData(event, meta)
        }
      } else {
        // for backwards compatibility, when table was not altered, meta columns not added
        event // no meta data
      }
    }
  }
}

/**
 * The retry policy that is used for reads, writes and deletes with
 * configured number of retries before giving up.
 * See http://docs.datastax.com/en/developer/java-driver/3.1/manual/retries/
 */
class FixedRetryPolicy(number: Int) extends RetryPolicy {
  override def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int,
                             nbRetry: Int): RetryDecision = {
    // Same implementation as in DefaultRetryPolicy
    // If this is the first retry it triggers a retry on the next host.
    // The rationale is that the first coordinator might have been network-isolated from all other nodes (thinking
    // they're down), but still able to communicate with the client; in that case, retrying on the same host has almost
    // no chance of success, but moving to the next host might solve the issue.
    if (nbRetry == 0)
      tryNextHost(cl, nbRetry) // see DefaultRetryPolicy
    else
      retry(cl, nbRetry)
  }

  override def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int,
                              receivedAcks: Int, nbRetry: Int): RetryDecision = {
    retry(cl, nbRetry)
  }

  override def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int,
                             dataRetrieved: Boolean, nbRetry: Int): RetryDecision = {
    retry(cl, nbRetry)
  }
  override def onRequestError(statement: Statement, cl: ConsistencyLevel, cause: DriverException,
                              nbRetry: Int): RetryDecision = {
    tryNextHost(cl, nbRetry)
  }

  private def retry(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.retry(cl) else RetryDecision.rethrow()
  }

  private def tryNextHost(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.tryNextHost(cl) else RetryDecision.rethrow()
  }

  override def init(c: Cluster): Unit = ()
  override def close(): Unit = ()

}
