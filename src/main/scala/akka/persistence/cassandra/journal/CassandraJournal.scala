/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.util.{ HashMap => JHMap, Map => JMap }
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import scala.concurrent._
import scala.math.min
import scala.util.control.NonFatal
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.nio.ByteBuffer
import java.lang.{ Long => JLong }
import java.util.concurrent.TimeUnit
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.serialization.SerializationExtension
import akka.actor.ActorRef
import akka.Done
import akka.actor.NoSerializationVerificationNeeded
import akka.serialization.Serialization
import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest

class CassandraJournal(cfg: Config) extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {

  import CassandraJournal._
  val config = new CassandraJournalConfig(cfg)
  val serialization = SerializationExtension(context.system)

  import context.dispatcher
  import config._
  import CassandraJournal._

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

  private[journal] class CassandraSession(val underlying: Session) {

    executeCreateKeyspaceAndTables(underlying, config.keyspaceAutoCreate, maxTagId)

    val preparedWriteMessage = underlying.prepare(writeMessage)
    val preparedDeleteMessages = underlying.prepare(deleteMessages)
    val preparedSelectMessages = underlying.prepare(selectMessages).setConsistencyLevel(readConsistency)
    val preparedCheckInUse = underlying.prepare(selectInUse).setConsistencyLevel(readConsistency)
    val preparedWriteInUse = underlying.prepare(writeInUse)
    val preparedSelectHighestSequenceNr = underlying.prepare(selectHighestSequenceNr).setConsistencyLevel(readConsistency)
    val preparedSelectDeletedTo = underlying.prepare(selectDeletedTo).setConsistencyLevel(readConsistency)
    val preparedInsertDeletedTo = underlying.prepare(insertDeletedTo).setConsistencyLevel(writeConsistency)

    def protocolVersion: ProtocolVersion =
      underlying.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
  }

  private var sessionUsed = false
  private val metricsCategory = s"${self.path.name}"

  private[journal] lazy val cassandraSession: CassandraSession = {
    retry(config.connectionRetries + 1, config.connectionRetryDelay.toMillis) {
      val underlying: Session = clusterBuilder.build().connect()
      CassandraMetricsRegistry(context.system).addMetrics(metricsCategory, underlying.getCluster.getMetrics.getRegistry)
      try {
        val s = new CassandraSession(underlying)
        new CassandraConfigChecker {
          override def session: Session = s.underlying
          override def config: CassandraJournalConfig = CassandraJournal.this.config
        }.initializePersistentConfig()
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
    CassandraMetricsRegistry(context.system).removeMetrics(metricsCategory)
  } catch {
    case NonFatal(_) => // nothing we can do
  }

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! CassandraJournal.Init
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)
    case CassandraJournal.Init =>
      try {
        cassandraSession
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
            e.getMessage
          )
      }
  }

  private val writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.writeRetries))
  private val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(config.deleteRetries))

  override def postStop(): Unit = {
    if (sessionUsed)
      closeSession(cassandraSession.underlying)
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
            case Tagged(payload, tags) =>
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
      case 1 => execute(boundStatements.head, writeRetryPolicy)
      case 0 => Future.successful(())
      case _ => executeBatch(batch => boundStatements.foreach(batch.add), writeRetryPolicy)
    }
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
    import cassandraSession._
    val maxPnr = partitionNr(atomicWrites.last.payload.last.sequenceNr)
    val firstSeq = atomicWrites.head.payload.head.sequenceNr
    val minPnr = partitionNr(firstSeq)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      // use same clock source as the UUID for the timeBucket
      val nowUuid = UUIDs.timeBased()
      val now = UUIDs.unixTimestamp(nowUuid)
      val bs = preparedWriteMessage.bind()
      bs.setString("persistence_id", persistenceId)
      bs.setLong("partition_nr", maxPnr)
      bs.setLong("sequence_nr", m.sequenceNr)
      bs.setUUID("timestamp", nowUuid)
      bs.setString("timebucket", TimeBucket(now).key)
      bs.setString("writer_uuid", m.writerUuid)
      bs.setInt("ser_id", m.serId)
      bs.setString("ser_manifest", m.serManifest)
      bs.setString("event_manifest", m.eventManifest)
      bs.setBytes("event", m.serialized)
      // for backwards compatibility
      bs.setToNull("message")
      if (cassandraSession.protocolVersion.compareTo(ProtocolVersion.V4) < 0) {
        (1 to maxTagsPerEvent).foreach(tagId => bs.setToNull("tag" + tagId))
      }

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
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSeq) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(persistenceId, minPnr: JLong)
    else writes

  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    writeInProgress.get(persistenceId) match {
      case null => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr)
      case f    => f.flatMap(_ => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr))
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    // TODO issue #6: use async api
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val lowestPartition = partitionNr(fromSequenceNr)
    val toSeqNr = math.min(toSequenceNr, readHighestSequenceNr(persistenceId, fromSequenceNr))
    val highestPartition = partitionNr(toSeqNr) + 1 // may have been moved to the next partition
    val partitionInfos = (lowestPartition to highestPartition).map(partitionInfo(persistenceId, _, toSeqNr))

    val logicalDelete = session.executeAsync(
      cassandraSession.preparedInsertDeletedTo.bind(persistenceId, toSeqNr: JLong)
    )

    if (config.cassandra2xCompat) {
      def asyncDeleteMessages(partitionNr: Long, messageIds: Seq[MessageId]): Future[Unit] = executeBatch({ batch =>
        messageIds.foreach { mid =>
          batch.add(cassandraSession.preparedDeleteMessages
            .bind(mid.persistenceId, partitionNr: JLong, mid.sequenceNr: JLong))
        }
      }, deleteRetryPolicy)

      partitionInfos.map(future => future.flatMap(pi => {
        Future.sequence((pi.minSequenceNr to pi.maxSequenceNr).grouped(config.maxMessageBatchSize).map { group =>
          {
            val delete = asyncDeleteMessages(pi.partitionNr, group map (MessageId(persistenceId, _)))
            delete.onFailure {
              case e => log.warning(s"Unable to complete deletes for persistence id ${persistenceId}, toSequenceNr ${toSequenceNr}. The plugin will continue to function correctly but you will need to manually delete the old messages.", e)
            }
            delete
          }
        })
      }))

    } else {
      Future.sequence(partitionInfos.map(future => future.flatMap { pi =>
        execute(
          cassandraSession.preparedDeleteMessages.bind(persistenceId, pi.partitionNr: JLong, toSeqNr: JLong),
          deleteRetryPolicy
        )
      })).onFailure {
        case e => log.warning(s"Unable to complete deletes for persistence id ${persistenceId}, " +
          s"toSequenceNr ${toSequenceNr}. The plugin will continue to " +
          "function correctly but you will need to manually delete the old messages.", e)
      }
    }

    logicalDelete.map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionInfo(persistenceId: String, partitionNr: Long, maxSequenceNr: Long): Future[PartitionInfo] = {
    session.executeAsync(cassandraSession.preparedSelectHighestSequenceNr
      .bind(persistenceId, partitionNr: JLong))
      .map(rs => Option(rs.one()))
      .map(row => row.map(s => PartitionInfo(partitionNr, minSequenceNr(partitionNr), min(s.getLong("sequence_nr"), maxSequenceNr)))
        .getOrElse(PartitionInfo(partitionNr, minSequenceNr(partitionNr), -1)))
  }

  private def executeBatch(body: BatchStatement ⇒ Unit, retryPolicy: RetryPolicy): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def execute(stmt: Statement, retryPolicy: RetryPolicy): Future[Unit] = {
    stmt.setConsistencyLevel(writeConsistency).setRetryPolicy(retryPolicy)
    session.executeAsync(stmt).map(_ => ())
  }

  private def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    findHighestSequenceNr(persistenceId, math.max(fromSequenceNr, highestDeletedSequenceNumber(persistenceId)))
  }

  private def readLowestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    new MessageIterator(persistenceId, fromSequenceNr, Long.MaxValue, Long.MaxValue).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)
  }

  private def findHighestSequenceNr(persistenceId: String, fromSequenceNr: Long) = {
    import cassandraSession._
    @annotation.tailrec
    def find(currentPnr: Long, currentSnr: Long): Long = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val next = Option(session.execute(preparedSelectHighestSequenceNr.bind(persistenceId, currentPnr: JLong)).one())
        .map(row => (row.getBool("used"), row.getLong("sequence_nr")))
      next match {
        // never been to this partition
        case None                   => currentSnr
        // don't currently explicitly set false
        case Some((false, _))       => currentSnr
        // everything deleted in this partition, move to the next
        case Some((true, 0))        => find(currentPnr + 1, currentSnr)
        case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
      }
    }
    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  private def highestDeletedSequenceNumber(persistenceId: String): Long = {
    Option(session.execute(cassandraSession.preparedSelectDeletedTo.bind(persistenceId)).one())
      .map(_.getLong("deleted_to")).getOrElse(0)
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
    def doSerializeEvent(): Serialized = {
      val event: AnyRef = p.payload.asInstanceOf[AnyRef]
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
        serializer.identifier, p.writerUuid)
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) ⇒ Serialization.currentTransportInformation.withValue(ti) { doSerializeEvent() }
      case None     ⇒ doSerializeEvent()
    }
  }

  private def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  // FIXME: Only used in readLowestSequenceNr. Optimize for the use case.
  private class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

    import PersistentRepr.Undefined
    import CassandraJournal.deserializeEvent

    private val initialFromSequenceNr = math.max(highestDeletedSequenceNumber(persistenceId) + 1, fromSequenceNr)
    log.debug("Starting message scan from {}", initialFromSequenceNr)

    private val iter = new RowIterator(persistenceId, initialFromSequenceNr, toSequenceNr)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    private var n: PersistentRepr = PersistentRepr(Undefined)

    fetch()

    def hasNext: Boolean =
      n != null && mcnt < max

    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      c
    }

    /**
     * Make next message n the current message c, complete c
     * and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val snr = row.getLong("sequence_nr")
        val m = row.getBytes("message") match {
          case null =>
            PersistentRepr(
              payload = deserializeEvent(serialization, row),
              sequenceNr = row.getLong("sequence_nr"),
              persistenceId = row.getString("persistence_id"),
              manifest = row.getString("event_manifest"),
              deleted = false,
              sender = null,
              writerUuid = row.getString("writer_uuid")
            )
          case b =>
            // for backwards compatibility
            persistentFromByteBuffer(b)
        }
        // there may be duplicates returned by iter
        // (on scan boundaries within a partition)
        if (snr == c.sequenceNr) c = m else n = m
      }
    }
  }

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  private class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    import cassandraSession._
    var currentPnr = partitionNr(fromSequenceNr)
    var currentSnr = fromSequenceNr

    var fromSnr = fromSequenceNr
    var toSnr = toSequenceNr

    var iter = newIter()

    def newIter() = {
      session.execute(preparedSelectMessages.bind(persistenceId, currentPnr: JLong, fromSnr: JLong, toSnr: JLong)).iterator
    }

    def inUse: Boolean = {
      val execute: ResultSet = session.execute(preparedCheckInUse.bind(persistenceId, currentPnr: JLong))
      if (execute.isExhausted) false
      else execute.one().getBool("used")
    }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (iter.hasNext) {
        // more entries available in current resultset
        true
      } else if (!inUse) {
        // partition has never been in use so stop
        false
      } else {
        // all entries consumed, try next partition
        currentPnr += 1
        fromSnr = currentSnr
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      currentSnr = row.getLong("sequence_nr")
      row
    }

    private def sequenceNrMin(partitionNr: Long): Long =
      partitionNr * targetPartitionSize + 1L

    private def sequenceNrMax(partitionNr: Long): Long =
      (partitionNr + 1L) * targetPartitionSize
  }
}

private[cassandra] object CassandraJournal {
  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded
  private case class MessageId(persistenceId: String, sequenceNr: Long)
  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                eventManifest: String, serManifest: String, serId: Int, writerUuid: String)
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)

  def deserializeEvent(serialization: Serialization, row: Row): Any = {
    serialization.deserialize(
      row.getBytes("event").array,
      row.getInt("ser_id"),
      row.getString("ser_manifest")
    ).get
  }
}

class FixedRetryPolicy(number: Int) extends RetryPolicy {
  override def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  override def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  override def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  override def onRequestError(statement: Statement, cl: ConsistencyLevel, cause: DriverException, nbRetry: Int): RetryDecision = retry(cl, nbRetry)

  private def retry(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.retry(cl) else RetryDecision.rethrow()
  }

  override def init(c: Cluster): Unit = ()
  override def close(): Unit = ()

}
