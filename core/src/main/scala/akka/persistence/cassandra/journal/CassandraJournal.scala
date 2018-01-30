/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.{ UUID, HashMap => JHMap, Map => JMap }

import akka.Done
import akka.actor.{ ActorRef, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.event.Logging
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TagWriter.TagWrite
import akka.persistence.cassandra.journal.TagWriters.{ BulkTagWrite, TagWritersSession }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.journal.{ AsyncWriteJournal, Tagged }
import akka.persistence.query.PersistenceQuery
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.utils.{ Bytes, UUIDs }
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.math.min
import scala.util.{ Failure, Success, Try }

class CassandraJournal(cfg: Config) extends AsyncWriteJournal
  with CassandraRecovery
  with CassandraStatements with NoSerializationVerificationNeeded {

  val config = new CassandraJournalConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)
  val ec: ExecutionContext = context.system.dispatcher
  val log = Logging(context.system, getClass)

  import CassandraJournal._
  import config._
  import context.dispatcher

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
      .flatMap(_ => initializePersistentConfig(session).map(_ => Done))
  )

  private val tagWriterSession = TagWritersSession(
    preparedWriteToTagViewWithoutMeta,
    preparedWriteToTagViewWithMeta,
    session.executeWrite,
    session.selectResultSet,
    preparedWriteToTagProgress
  )

  protected val tagWrites: Option[ActorRef] =
    if (config.eventsByTagEnabled)
      Some(context.system.actorOf(TagWriters.props((arf, tag) => arf.actorOf(TagWriter.props(tagWriterSession, tag, config.tagWriterSettings)))))
    else None

  def preparedWriteMessage = session.prepare(
    writeMessage(withMeta = false)
  ).map(_.setIdempotent(true))

  def preparedWriteMessageWithMeta = session.prepare(
    writeMessage(withMeta = true)
  ).map(_.setIdempotent(true))

  def preparedDeleteMessages = session.prepare(deleteMessages).map(_.setIdempotent(true))
  def preparedSelectMessages = session.prepare(selectMessages)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedCheckInUse = session.prepare(selectInUse)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true))
  def preparedWriteInUse = session.prepare(writeInUse).map(_.setIdempotent(true))
  def preparedSelectHighestSequenceNr = session.prepare(selectHighestSequenceNr)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedSelectDeletedTo = session.prepare(selectDeletedTo)
    .map(_.setConsistencyLevel(readConsistency).setIdempotent(true).setRetryPolicy(readRetryPolicy))
  def preparedInsertDeletedTo = session.prepare(insertDeletedTo)
    .map(_.setConsistencyLevel(writeConsistency).setIdempotent(true))

  private[akka] implicit val materializer = ActorMaterializer()
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
      preparedCheckInUse
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
      }
  }

  private val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  private val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))
  private val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.readRetries))
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

    tagWrites.foreach(tws pipeTo _)

    tws.onComplete { _ =>
      self ! WriteFinished(pid, p.future)
      p.success(Done)
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

    //Nil == all good
    tws.map(_ => Nil)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
  }

  // FIXME optimize case where there is only one write/tag
  private def extractTagWrites(serialized: Seq[SerializedAtomicWrite]) = {
    val messagesByTag: Map[String, Seq[Serialized]] = serialized
      .flatMap(_.payload)
      .flatMap(s => s.tags.map((_, s)))
      .groupBy(_._1).mapValues(_.map(_._2))

    BulkTagWrite(
      messagesByTag.map {
      case (tag, writes) =>
        TagWrite(tag, writes.toVector)
    }.toVector
    )
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
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq)
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
          bs.setSet("tags", m.tags, classOf[String])

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

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    writeInProgress.get(persistenceId) match {
      case null => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr)
      case f    => f.flatMap(_ => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr))
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap {
      highestDeletedSequenceNumber =>
        asyncReadLowestSequenceNr(persistenceId, 1L, highestDeletedSequenceNumber).flatMap {
          lowestSequenceNr =>
            super.asyncReadHighestSequenceNr(persistenceId, lowestSequenceNr).flatMap {
              highestSequenceNr =>
                val lowestPartition = partitionNr(lowestSequenceNr)
                val toSeqNr = math.min(toSequenceNr, highestSequenceNr)
                val highestPartition = partitionNr(toSeqNr) + 1 // may have been moved to the next partition

                val boundInsertDeletedTo = preparedInsertDeletedTo.map(_.bind(persistenceId, toSeqNr: JLong))
                val logicalDelete = boundInsertDeletedTo.flatMap(session.executeWrite)

                def partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))

                def physicalDelete(): Unit = {
                  if (config.cassandra2xCompat) {
                    def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = {
                      val boundStatements = messageIds.map(mid =>
                        preparedDeleteMessages.map(_.bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong)))
                      Future.sequence(boundStatements).flatMap {
                        stmts =>
                          executeBatch(batch => stmts.foreach(batch.add), deleteRetryPolicy)
                      }
                    }

                    partitionInfos.map(future => future.flatMap(pi => {
                      Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map {
                        group =>
                          {
                            val delete = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
                            delete.onFailure {
                              case e => log.warning(s"Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                                "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                                "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
                            }
                            delete
                          }
                      })
                    }))

                  } else {
                    Future.sequence(partitionInfos.map(future => future.flatMap {
                      pi =>
                        val boundDeleteMessages = preparedDeleteMessages.map(_.bind(persistenceId, pi.partitionNr: JLong, toSeqNr: JLong))
                        boundDeleteMessages.flatMap(execute(_, deleteRetryPolicy))
                    }))
                      .onFailure {
                        case e => log.warning("Unable to complete deletes for persistence id {}, toSequenceNr {}. " +
                          "The plugin will continue to function correctly but you will need to manually delete the old messages. " +
                          "Caused by: [{}: {}]", persistenceId, toSequenceNr, e.getClass.getName, e.getMessage)
                      }
                  }
                }

                logicalDelete.foreach(_ => physicalDelete())

                logicalDelete.map(_ => Done)
            }
        }
    }
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, partitionNr: JLong))
    boundSelectHighestSequenceNr.flatMap(session.selectResultSet)
      .map(rs => Option(rs.one()))
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
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

  // TODO create a custom extractor that doesn't deserialise the event
  private def asyncReadLowestSequenceNr(persistenceId: String, fromSequenceNr: Long, highestDeletedSequenceNumber: Long): Future[Long] = {
    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        highestDeletedSequenceNumber,
        1,
        1,
        None,
        "asyncReadLowestSequenceNr",
        someReadConsistency,
        someReadRetryPolicy,
        extractor = Extractors.taggedPersistentRepr
      )
      .map(_.pr.sequenceNr)
      .runWith(Sink.headOption)
      .map {
        case Some(sequenceNr) => sequenceNr
        case None             => fromSequenceNr
      }
  }

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1

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
  private case class MessageId(persistenceId: String, sequenceNr: Long)

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])

  private[akka] case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                      eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
                                      meta: Option[SerializedMeta], timeUuid: UUID, timeBucket: TimeBucket)

  private[akka] case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)

  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)

  class EventDeserializer(serialization: Serialization) {

    // cache to avoid repeated check via ColumnDefinitions
    @volatile private var _hasMetaColumns: Option[Boolean] = None

    def hasMetaColumns(row: Row): Boolean = _hasMetaColumns match {
      case Some(b) => b
      case None =>
        val b = row.getColumnDefinitions.contains("meta")
        _hasMetaColumns = Some(b)
        b
    }

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
