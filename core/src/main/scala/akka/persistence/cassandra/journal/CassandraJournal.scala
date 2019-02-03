/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.{ UUID, HashMap => JHMap, Map => JMap }

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence._
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TagWriters.{ BulkTagWrite, TagWrite, TagWritersSession }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.query.PersistenceQuery
import akka.serialization.{ AsyncSerializer, Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.OptionVal
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.utils.{ Bytes, UUIDs }
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * Journal implementation of the cassandra plugin.
 * Inheritance is possible but without any guarantees for future source compatibility.
 */
@DoNotInherit
class CassandraJournal(cfg: Config) extends AsyncWriteJournal
  with CassandraRecovery
  with CassandraStatements
  with NoSerializationVerificationNeeded {

  val config = new CassandraJournalConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)
  val log: LoggingAdapter = Logging(context.system, getClass)

  private lazy val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))

  import CassandraJournal._
  import config._

  implicit override val ec: ExecutionContextExecutor = context.dispatcher

  // readHighestSequence must be performed after pending write for a persistenceId
  // when the persistent actor is restarted.
  // It seems like C* doesn't support session consistency so we handle it ourselves.
  // https://aphyr.com/posts/299-the-trouble-with-timestamps
  private val writeInProgress: JMap[String, Future[Done]] = new JHMap

  // Can't think of a reason why we can't have writes and deletes
  // run concurrently. This should be a very infrequently used
  // so fine to use an immutable list as the value
  private val pendingDeletes: JMap[String, List[PendingDelete]] = new JHMap

  val session = new CassandraSession(
    context.system,
    config.sessionProvider,
    config.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = s"${self.path.name}",
    init = session =>
      executeCreateKeyspaceAndTables(session, config))

  private val tagWriterSession = TagWritersSession(
    () => preparedWriteToTagViewWithoutMeta,
    () => preparedWriteToTagViewWithMeta,
    session.executeWrite,
    session.selectResultSet,
    () => preparedWriteToTagProgress,
    () => preparedWriteTagScanning)

  protected val tagWrites: Option[ActorRef] =
    if (config.eventsByTagEnabled)
      Some(context.actorOf(TagWriters.props(config.tagWriterSettings, tagWriterSession)
        .withDispatcher(context.props.dispatcher), "tagWrites"))
    else None

  def preparedWriteMessage = session.prepare(writeMessage(withMeta = false)).map(_.setIdempotent(true))
  def preparedSelectDeletedTo = session.prepare(selectDeletedTo)
    .map(_.setConsistencyLevel(config.readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedSelectHighestSequenceNr = session.prepare(selectHighestSequenceNr)
    .map(_.setConsistencyLevel(config.readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedInsertDeletedTo = session.prepare(insertDeletedTo)
    .map(_.setConsistencyLevel(config.writeConsistency).setIdempotent(true))
  def preparedDeleteMessages = session.prepare(deleteMessages).map(_.setIdempotent(true))

  def preparedWriteMessageWithMeta = session.prepare(
    writeMessage(withMeta = true)).map(_.setIdempotent(true))
  def preparedSelectMessages = session.prepare(selectMessages)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedWriteInUse = session.prepare(writeInUse).map(_.setIdempotent(true))

  implicit val materializer: ActorMaterializer = ActorMaterializer()(context.system)

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

    case DeleteFinished(persistenceId, toSequenceNr, result) =>
      log.debug("Delete finished for persistence id [{}] to [{}] result [{}]", persistenceId, toSequenceNr, result)
      pendingDeletes.get(persistenceId) match {
        case null =>
          log.error("Delete finished but not in pending. Please raise a bug with logs. PersistenceId: [{}]", persistenceId)
        case Nil =>
          log.error("Delete finished but not in pending (empty). Please raise a bug with logs. PersistenceId: [{}]", persistenceId)
        case current :: tail =>
          current.p.complete(result)
          tail match {
            case Nil =>
              pendingDeletes.remove(persistenceId)
            case next :: _ =>
              pendingDeletes.put(persistenceId, tail)
              delete(next.pid, next.toSequenceNr)
          }
      }

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
      queries.initialize()

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
    def serialize(aw: Seq[(PersistentRepr, UUID)]): Future[SerializedAtomicWrite] = {
      val serializedEventsFut: Future[Seq[Serialized]] = Future.sequence(aw.map {
        case (pr, uuid) =>
          val (pr2, tags) = pr.payload match {
            case Tagged(payload, ts) =>
              (pr.withPayload(payload), ts)
            case _ =>
              (pr, Set.empty[String])
          }
          serializeEvent(pr2, tags, uuid, config.bucketSize, serialization, context.system)
      })

      serializedEventsFut.map { serializedEvents =>
        SerializedAtomicWrite(aw.head._1.persistenceId, serializedEvents)
      }
    }

    val writesWithUuids: Seq[Seq[(PersistentRepr, UUID)]] = messages
      .map(aw => aw.payload.map(pr => (pr, generateUUID(pr))))

    val p = Promise[Done]
    val pid = messages.head.persistenceId
    writeInProgress.put(pid, p.future)

    val tws = Future.sequence(writesWithUuids.map(w => serialize(w))).flatMap { serialized: Seq[SerializedAtomicWrite] =>
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

  /**
   * UUID generation is deliberately externalized to allow subclasses to customize the time based uuid for special cases.
   * see https://discuss.lightbend.com/t/akka-persistence-cassandra-events-by-tags-bucket-size-based-on-time-vs-burst-load/1411 and make sure you understand the risk of doing this wrong.
   */
  protected def generateUUID(pr: PersistentRepr): UUID = UUIDs.timeBased()

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
      val persistentActor = sender()
      highestSequenceNr.flatMap { seqNr =>
        if (seqNr == fromSequenceNr && seqNr != 0) {
          log.debug("Snapshot is current so replay won't be required. Calculating tag progress now.")
          val scanningSeqNrFut = tagScanningStartingSequenceNr(persistenceId)
          for {
            tp <- lookupTagProgress(persistenceId)
            _ <- persistenceIdStarting(persistenceId, tp, tagWrites.get, persistentActor)
            scanningSeqNr <- scanningSeqNrFut
            _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, Long.MaxValue, tp)
          } yield seqNr
        } else if (seqNr == 0) {
          log.debug("New persistenceId [{}]. Sending blank tag progress.", persistenceId)
          persistenceIdStarting(persistenceId, Map.empty, tagWrites.get, persistentActor).map(_ => seqNr)
        } else {
          highestSequenceNr
        }
      }
    } else {
      highestSequenceNr
    }
  }

  /**
   * Not thread safe. Assumed to only be called from the journal actor.
   * However, unlike asyncWriteMessages it can be called before the previous Future completes
   */
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    // TODO could "optimize" away deletes that overlap?
    pendingDeletes.get(persistenceId) match {
      case null =>
        log.debug("No outstanding delete for persistence id {}. Sequence nr: {}", persistenceId, toSequenceNr)
        // fast path, no outstanding deletes for this persistenceId
        val p = Promise[Unit]()
        pendingDeletes.put(persistenceId, List(PendingDelete(persistenceId, toSequenceNr, p)))
        delete(persistenceId, toSequenceNr)
        p.future
      case otherDeletes =>
        // Users really should not be firing deletes this quickly
        if (otherDeletes.length > config.maxConcurrentDeletes) {
          log.error("Over [{}] outstanding deletes for persistenceId [{}]. Failing delete", config.maxConcurrentDeletes, persistenceId)
          Future.failed(new RuntimeException(s"Over ${config.maxConcurrentDeletes} outstanding deletes for persistenceId $persistenceId"))
        } else {
          log.debug("Outstanding delete for persistenceId [{}]. Delete to [{}] will be scheduled after previous one finished.", persistenceId, toSequenceNr)
          val p = Promise[Unit]()
          pendingDeletes.put(persistenceId, otherDeletes :+ PendingDelete(persistenceId, toSequenceNr, p))
          p.future
        }
    }
  }

  private def delete(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    def physicalDelete(lowestPartition: Long, highestPartition: Long, toSeqNr: Long): Future[Done] = {
      if (config.cassandra2xCompat) {
        def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = {
          val boundStatements = messageIds.map(mid =>
            preparedDeleteMessages.map(_.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong)))
          Future.sequence(boundStatements).flatMap { stmts =>
            executeBatch(batch => stmts.foreach(batch.add), deleteRetryPolicy)
          }
        }

        val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))
        val deleteResult =
          Future.sequence(partitionInfos.map(future => future.flatMap(pi => {
            Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map {
              group =>
                {
                  val groupDeleteResult = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
                  groupDeleteResult.failed.foreach { e =>
                    log.warning(s"Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                      "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                      "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
                  }
                  groupDeleteResult
                }
            })
          })))
        deleteResult.map(_ => Done)

      } else {
        val deleteResult = Future.sequence((lowestPartition to highestPartition).map { partitionNr =>
          val boundDeleteMessages = preparedDeleteMessages.map(_.bind(persistenceId, partitionNr: JLong, toSeqNr: JLong))
          boundDeleteMessages.flatMap(execute(_, deleteRetryPolicy))
        })
        deleteResult.failed.foreach { e =>
          log.warning("Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
            "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
            "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
        }
        deleteResult.map(_ => Done)
      }
    }

    /**
     * Deletes the events by inserting into the metadata table deleted_to
     * and physically deletes the rows.
     */
    def logicalAndPhysicalDelete(highestDeletedSequenceNumber: Long, highestSequenceNr: Long): Future[Done] = {
      val lowestPartition = partitionNr(highestDeletedSequenceNumber + 1, config.targetPartitionSize)
      val toSeqNr = math.min(toSequenceNr, highestSequenceNr)
      val highestPartition = partitionNr(toSeqNr, config.targetPartitionSize) + 1 // may have been moved to the next partition
      val logicalDelete =
        if (toSeqNr <= highestDeletedSequenceNumber) {
          // already deleted same or higher sequence number, don't update highestDeletedSequenceNumber,
          // but perform the physical delete (again), may be a retry request
          Future.successful(())
        } else {
          val boundInsertDeletedTo = preparedInsertDeletedTo.map(_.bind(persistenceId, toSeqNr: JLong))
          boundInsertDeletedTo.flatMap(session.executeWrite)
        }
      logicalDelete.flatMap(_ => physicalDelete(lowestPartition, highestPartition, toSeqNr))
    }

    val deleteResult = for {
      highestDeletedSequenceNumber <- asyncHighestDeletedSequenceNumber(persistenceId)
      highestSequenceNr <- {
        // MaxValue may be used as magic value to delete all events without specifying actual toSequenceNr
        if (toSequenceNr == Long.MaxValue)
          asyncFindHighestSequenceNr(persistenceId, highestDeletedSequenceNumber, config.targetPartitionSize)
        else
          Future.successful(toSequenceNr)
      }
      _ <- logicalAndPhysicalDelete(highestDeletedSequenceNumber, highestSequenceNr)
    } yield ()

    // Kick off any pending deletes when finished.
    deleteResult.onComplete { result => self ! DeleteFinished(persistenceId, toSequenceNr, result) }

    deleteResult
  }

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, partitionNr: JLong))
    boundSelectHighestSequenceNr.flatMap(session.selectOne)
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), math.min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private[akka] def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    val boundSelectDeletedTo = preparedSelectDeletedTo.map(_.bind(persistenceId))
    boundSelectDeletedTo.flatMap(session.selectOne)
      .map(rowOption => rowOption.map(_.getLong("deleted_to")).getOrElse(0))
  }

  private[akka] def asyncReadLowestSequenceNr(
    persistenceId:                String,
    fromSequenceNr:               Long,
    highestDeletedSequenceNumber: Long,
    readConsistency:              Option[ConsistencyLevel],
    retryPolicy:                  Option[RetryPolicy]): Future[Long] = {
    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        highestDeletedSequenceNumber,
        1,
        1,
        None,
        "asyncReadLowestSequenceNr",
        readConsistency,
        retryPolicy,
        extractor = Extractors.sequenceNumber(eventDeserializer, serialization))
      .map(_.sequenceNr)
      .runWith(Sink.headOption)
      .map {
        case Some(sequenceNr) => sequenceNr
        case None             => fromSequenceNr
      }
  }

  private[akka] def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long, partitionSize: Long): Future[Long] = {
    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, currentPnr: JLong))
      boundSelectHighestSequenceNr.flatMap(session.selectOne)
        .map { rowOption =>
          rowOption.map { row =>
            (row.getBool("used"), row.getLong("sequence_nr"))
          }
        }
        .flatMap {
          // never been to this partition
          case None                   => Future.successful(currentSnr)
          // don't currently explicitly set false
          case Some((false, _))       => Future.successful(currentSnr)
          // everything deleted in this partition, move to the next
          case Some((true, 0))        => find(currentPnr + 1, currentSnr)
          case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
        }
    }

    find(partitionNr(fromSequenceNr, partitionSize), fromSequenceNr)
  }

  private def executeBatch(body: BatchStatement ⇒ Unit, retryPolicy: RetryPolicy): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy).asInstanceOf[BatchStatement]
    body(batch)
    session.underlying().flatMap(_.executeAsync(batch).asScala).map(_ => ())
  }

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * config.targetPartitionSize + 1

  private def execute(stmt: Statement, retryPolicy: RetryPolicy): Future[Unit] = {
    stmt.setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy)
    session.executeWrite(stmt).map(_ => ())
  }

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraJournal {
  private[akka] type Tag = String
  private[akka] type PersistenceId = String
  private[akka] type SequenceNr = Long
  private[akka] type TagPidSequenceNr = Long

  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded

  private case class DeleteFinished(pid: String, toSequenceNr: Long, f: Try[Unit]) extends NoSerializationVerificationNeeded
  private case class PendingDelete(pid: String, toSequenceNr: Long, p: Promise[Unit]) extends NoSerializationVerificationNeeded

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])

  private[akka] case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                      eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
                                      meta: Option[SerializedMeta], timeUuid: UUID, timeBucket: TimeBucket)

  private[akka] case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
  private case class MessageId(persistenceId: String, sequenceNr: Long)

  class EventDeserializer(system: ActorSystem) {

    private val serialization = SerializationExtension(system)

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

    def deserializeEvent(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[Any] = try {

      def meta: OptionVal[AnyRef] = {
        if (hasMetaColumns(row)) {
          row.getBytes("meta") match {
            case null =>
              OptionVal.None // no meta data
            case metaBytes =>
              // has meta data, wrap in EventWithMetaData
              val metaSerId = row.getInt("meta_ser_id")
              val metaSerManifest = row.getString("meta_ser_manifest")
              val meta = serialization.deserialize(
                Bytes.getArray(metaBytes),
                metaSerId,
                metaSerManifest) match {
                  case Success(m) => m
                  case Failure(_) =>
                    // don't fail replay/query because of deserialization problem with meta data
                    // see motivation in UnknownMetaData
                    UnknownMetaData(metaSerId, metaSerManifest)
                }
              OptionVal.Some(meta)
          }
        } else {
          // for backwards compatibility, when table was not altered, meta columns not added
          OptionVal.None // no meta data
        }
      }

      val bytes = Bytes.getArray(row.getBytes("event"))
      val serId = row.getInt("ser_id")
      val manifest = row.getString("ser_manifest")

      serialization.serializerByIdentity.get(serId) match {
        case Some(asyncSerializer: AsyncSerializer) =>
          Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSerializer.fromBinaryAsync(bytes, manifest).map { event =>
              meta match {
                case OptionVal.None    => event
                case OptionVal.Some(m) => EventWithMetaData(event, m)
              }
            }
          }

        case _ =>
          def deserializedEvent: AnyRef = {
            // Serialization.deserialize adds transport info
            val event = serialization.deserialize(bytes, serId, manifest).get
            meta match {
              case OptionVal.None    => event
              case OptionVal.Some(m) => EventWithMetaData(event, m)
            }
          }

          if (async) Future(deserializedEvent)
          else Future.successful(deserializedEvent)
      }

    } catch {
      case NonFatal(e) => Future.failed(e)
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
