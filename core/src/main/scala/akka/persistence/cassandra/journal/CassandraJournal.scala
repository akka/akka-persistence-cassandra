/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.nio.ByteBuffer

import java.util.{ HashMap => JHMap, Map => JMap }
import java.lang.{ Long => JLong }

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.math.min
import scala.util.Try

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.stream.ActorMaterializer
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import scala.util.Success
import scala.util.Failure
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData

class CassandraJournal(cfg: Config) extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {

  import CassandraJournal._
  val config = new CassandraJournalConfig(context.system, cfg)
  val serialization = SerializationExtension(context.system)

  import config._
  import CassandraJournal._

  import context.dispatcher

  // readHighestSequence must be performed after pending write for a persistenceId
  // when the persistent actor is restarted.
  // It seems like C* doesn't support session consistency so we handle it ourselves.
  // https://aphyr.com/posts/299-the-trouble-with-timestamps
  private val writeInProgress: JMap[String, Future[Done]] = new JHMap

  // Announce written changes to DistributedPubSub if pubsub-minimum-interval is defined in config
  private val pubsub = pubsubMinimumInterval match {
    case interval: FiniteDuration =>
      // PubSub will be ignored when clustering is unavailable
      Try {
        DistributedPubSub(context.system)
      }.toOption flatMap { extension =>
        if (extension.isTerminated)
          None
        else
          Some(context.actorOf(PubSubThrottler.props(extension.mediator, interval)
            .withDispatcher(context.props.dispatcher)))
      }

    case _ => None
  }

  val session = new CassandraSession(
    context.system,
    config.sessionProvider,
    config.sessionSettings,
    context.dispatcher,
    log,
    metricsCategory = s"${self.path.name}",
    init = session =>
    executeCreateKeyspaceAndTables(session, config, maxTagId)
      .flatMap(_ => initializePersistentConfig(session).map(_ => Done))
  )

  def preparedWriteMessage = session.prepare(
    if (config.enableEventsByTagQuery) writeMessage(withMeta = false) else writeMessageNoTags(withMeta = false)
  )
    .map(_.setIdempotent(true))
  def preparedWriteMessageWithMeta = session.prepare(
    if (config.enableEventsByTagQuery) writeMessage(withMeta = true) else writeMessageNoTags(withMeta = true)
  )
    .map(_.setIdempotent(true))
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

  private[cassandra] implicit val materializer = ActorMaterializer()
  private[cassandra] lazy val queries =
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
  }

  private val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  private val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))
  private val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.readRetries))
  private[akka] val someReadRetryPolicy = Some(readRetryPolicy)
  private[akka] val someReadConsistency = Some(config.readConsistency)

  override def postStop(): Unit = {
    session.close()
  }

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    //
    // We must NOT catch serialization exceptions here because rejections will cause
    // holes in the sequence number series and we use the sequence numbers to detect
    // missing (delayed) events in the eventByTag query.
    //
    // Note that we assume that all messages have the same persistenceId, which is
    // the case for Akka 2.4.2.

    def serialize(aw: AtomicWrite): SerializedAtomicWrite =
      SerializedAtomicWrite(
        aw.persistenceId,
        aw.payload.map { pr =>
          val (pr2, tags) = pr.payload match {
            case Tagged(payload, tags) if config.enableEventsByTagQuery =>
              (pr.withPayload(payload), tags)
            case _ => (pr, Set.empty[String])
          }
          serializeEvent(pr2, tags)
        }
      )

    def publishTagNotification(serialized: Seq[SerializedAtomicWrite], result: Future[_]): Unit = {
      if (pubsub.isDefined) {
        result.foreach { _ =>
          for (
            p <- pubsub;
            tag: String <- serialized.map(_.payload.map(_.tags).flatten).flatten.toSet
          ) {
            p ! DistributedPubSubMediator.Publish("akka.persistence.cassandra.journal.tag", tag)
          }
        }
      }
    }

    val p = Promise[Done]
    val pid = messages.head.persistenceId
    writeInProgress.put(pid, p.future)

    Future(messages.map(serialize)).flatMap { serialized =>
      val result =
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

      result.onComplete { _ =>
        self ! WriteFinished(pid, p.future)
        p.success(Done)
      }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

      publishTagNotification(serialized, result)
      // Nil == all good
      result.map(_ => Nil)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
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
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[Future[BoundStatement]] = all.map { m =>
      // use same clock source as the UUID for the timeBucket
      val nowUuid = UUIDs.timeBased()
      val now = UUIDs.unixTimestamp(nowUuid)

      // using two separate statements with or without the meta data columns because
      // then users doesn't have to alter table and add the new columns if they don't use
      // the meta data feature
      val stmt = if (m.meta.isDefined) preparedWriteMessageWithMeta else preparedWriteMessage

      stmt.map { stmt =>
        val bs = stmt.bind()
        bs.setString("persistence_id", persistenceId)
        bs.setLong("partition_nr", maxPnr)
        bs.setLong("sequence_nr", m.sequenceNr)
        bs.setUUID("timestamp", nowUuid)
        bs.setString("timebucket", TimeBucket(now).key)
        bs.setString("writer_uuid", m.writerUuid)
        bs.setInt("ser_id", m.serId)
        bs.setString("ser_manifest", m.serManifest)
        bs.setString("event_manifest", m.eventAdapterManifest)
        bs.setBytes("event", m.serialized)

        // meta data, if any
        m.meta match {
          case Some(meta) =>
            bs.setInt("meta_ser_id", meta.serId)
            bs.setString("meta_ser_manifest", meta.serManifest)
            bs.setBytes("meta", meta.serialized)
          case None =>
        }

        // tags, if any
        if (m.tags.nonEmpty) {
          var tagCounts = Array.ofDim[Int](maxTagsPerEvent)
          m.tags.foreach { tag =>
            val tagId = tags.getOrElse(tag, 1)
            bs.setString("tag" + tagId, tag)
            tagCounts(tagId - 1) = tagCounts(tagId - 1) + 1
            var i = 0
            while (i < tagCounts.length) {
              if (tagCounts(i) > 1)
                log.warning("Duplicate tag identifer [{}] among tags [{}] for event from [{}]. " +
                  "Define known tags in cassandra-journal.tags configuration when using more than " +
                  "one tag per event.", (i + 1), m.tags.mkString(","), persistenceId)
              i += 1
            }
          }
        }

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
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { highestDeletedSequenceNumber =>
      asyncReadLowestSequenceNr(persistenceId, 1L, highestDeletedSequenceNumber).flatMap { lowestSequenceNr =>
        super.asyncReadHighestSequenceNr(persistenceId, lowestSequenceNr).flatMap { highestSequenceNr =>
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
                Future.sequence(boundStatements).flatMap { stmts =>
                  executeBatch(batch => stmts.foreach(batch.add), deleteRetryPolicy)
                }
              }

              partitionInfos.map(future => future.flatMap(pi => {
                Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map { group =>
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
              Future.sequence(partitionInfos.map(future => future.flatMap { pi =>
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

  private def asyncReadLowestSequenceNr(persistenceId: String, fromSequenceNr: Long, highestDeletedSequenceNumber: Long): Future[Long] = {
    import context.dispatcher
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
        someReadRetryPolicy
      )
      .map(_.sequenceNr)
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

  private lazy val transportInformation: Option[Serialization.Information] = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, context.system))
  }

  private def serializeEvent(p: PersistentRepr, tags: Set[String]): Serialized = {

    def serializeMeta(): Option[SerializedMeta] = {
      // meta data, if any
      p.payload match {
        case EventWithMetaData(_, m) =>
          val m2 = m.asInstanceOf[AnyRef]
          val serializer = serialization.findSerializerFor(m2)
          val serManifest = serializer match {
            case ser2: SerializerWithStringManifest ⇒
              ser2.manifest(m2)
            case _ ⇒
              if (serializer.includeManifest) m2.getClass.getName
              else PersistentRepr.Undefined
          }
          val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
          Some(SerializedMeta(metaBuf, serManifest, serializer.identifier))
        case evt => None
      }
    }

    def doSerializeEvent(): Serialized = {
      val event: AnyRef = (p.payload match {
        case EventWithMetaData(evt, _) => evt // unwrap
        case evt                       => evt
      }).asInstanceOf[AnyRef]

      val serializer = serialization.findSerializerFor(event)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest ⇒
          ser2.manifest(event)
        case _ ⇒
          if (serializer.includeManifest) event.getClass.getName
          else PersistentRepr.Undefined
      }
      val serEvent = ByteBuffer.wrap(serialization.serialize(event).get)

      Serialized(p.persistenceId, p.sequenceNr, serEvent, tags, p.manifest, serManifest,
        serializer.identifier, p.writerUuid, serializeMeta())
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) ⇒ Serialization.currentTransportInformation.withValue(ti) { doSerializeEvent() }
      case None     ⇒ doSerializeEvent()
    }
  }

}

private[cassandra] object CassandraJournal {
  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded
  private case class MessageId(persistenceId: String, sequenceNr: Long)
  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
                                meta: Option[SerializedMeta])
  private case class SerializedMeta(serialized: ByteBuffer, serManifest: String, serId: Int)
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
        row.getBytes("event").array,
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
              metaBytes.array,
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
