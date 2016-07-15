/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import akka.persistence.journal.EventAdapters

import scala.concurrent.duration._
import scala.util.Try
import akka.actor.ActorLogging
import akka.actor.DeadLetterSuppression
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs
import akka.persistence.cassandra.journal.TimeBucket
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence.cassandra.PreparedStatementEnvelope

private[query] object EventsByTagPublisher {

  def props(tag: String, fromOffset: UUID, toOffset: Option[UUID], settings: CassandraReadJournalConfig,
            session: Session, preparedSelect: PreparedStatement): Props = {
    Props(classOf[EventsByTagPublisher],tag, fromOffset, toOffset,
      settings, PreparedStatementEnvelope(session,preparedSelect))
  }

  private[query] case object Continue extends DeadLetterSuppression

  private[query] case class ReplayDone(count: Int, seqNumbers: Option[SequenceNumbers], highest: UUID)
    extends DeadLetterSuppression
  private[query] case class ReplayAborted(
    seqNumbers: Option[SequenceNumbers], persistenceId: String, expectedSeqNr: Long, gotSeqNr: Long
  )
    extends DeadLetterSuppression
  private[query] final case class ReplayFailed(cause: Throwable)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

}

private[query] class EventsByTagPublisher(
  tag: String, fromOffset: UUID, toOffset: Option[UUID],
  settings: CassandraReadJournalConfig, preparedSelect: PreparedStatementEnvelope
)
  extends ActorPublisher[UUIDPersistentRepr] with DeliveryBuffer[UUIDPersistentRepr] with ActorLogging {
  import akka.persistence.cassandra.query.UUIDComparator.comparator.compare
  import EventsByTagPublisher._
  import settings.maxBufferSize
  import settings.refreshInterval
  import context.dispatcher

  val eventualConsistencyDelayMillis = settings.eventualConsistencyDelay.toMillis
  val toOffsetTimestamp = toOffset match {
    case Some(uuid) => UUIDs.unixTimestamp(uuid) + eventualConsistencyDelayMillis
    case None       => Long.MaxValue
  }

  // Subscribe to DistributedPubSub so we can receive immediate notifications when the journal has written something.
  if (settings.pubsubMinimumInterval.isFinite) {
    Try { // Ignore pubsub when clustering unavailable
      DistributedPubSub(context.system).mediator !
        DistributedPubSubMediator.Subscribe("akka.persistence.cassandra.journal.tag", self)
    }
  }

  var currTimeBucket: TimeBucket = TimeBucket(fromOffset)
  var currOffset: UUID = fromOffset
  var highestOffset: UUID = fromOffset
  val strictBySeqNumber = settings.delayedEventTimeout > Duration.Zero
  var seqNumbers: Option[SequenceNumbers] =
    if (strictBySeqNumber) Some(SequenceNumbers.empty)
    else None
  var abortDeadline: Option[Deadline] = None
  var lookForMissingDeadline: Deadline = nextLookForMissingDeadline()

  val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    onErrorThenStop(reason)

  override def postRestart(reason: Throwable): Unit =
    throw new IllegalStateException(s"$self must not be restarted")

  override def postStop(): Unit =
    tickTask.cancel()

  override def unhandled(msg: Any): Unit = msg match {
    case _: String =>
    // These are published to the pubsub topic, and can be safely ignored if not specifically handled.
    case _ =>
      super.unhandled(msg)
  }

  def nextTimeBucket(): Unit =
    currTimeBucket = currTimeBucket.next()

  def today(): LocalDate =
    LocalDateTime.now(ZoneOffset.UTC).minus(eventualConsistencyDelayMillis, ChronoUnit.MILLIS).toLocalDate

  def isTimeBucketBeforeToday(): Boolean =
    currTimeBucket.isBefore(today())

  def goBack(): Unit = {
    val timestamp = UUIDs.unixTimestamp(currOffset) - settings.delayedEventTimeout.toMillis
    val backFromOffset = UUIDs.startOf(timestamp)
    currOffset =
      if (compare(fromOffset, backFromOffset) >= 0) fromOffset
      else backFromOffset
    currTimeBucket = TimeBucket(currOffset)
  }

  def isBacktracking: Boolean =
    compare(currOffset, highestOffset) < 0

  def nextLookForMissingDeadline(): Deadline =
    Deadline.now + (settings.delayedEventTimeout / 2)

  // exceptions from Fetcher
  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
    case e =>
      log.debug("Query of eventsByTag [{}] failed, due to: {}", tag, e.getMessage)
      self ! ReplayFailed(e)
      SupervisorStrategy.Stop
  }

  def receive = init

  def init: Receive = {
    case _: Request => replay()
    case Continue   => // skip, wait for first Request
    case Cancel     => context.stop(self)
  }

  def idle: Receive = {
    case Continue =>
      if (strictBySeqNumber && !isBacktracking && lookForMissingDeadline.isOverdue()) {
        // look for delayed events
        goBack()
        lookForMissingDeadline = nextLookForMissingDeadline()
      }
      if (timeForReplay)
        replay()

    case _: Request =>
      deliverBuf()
      stopIfDone()

    case Cancel =>
      context.stop(self)

    case tagWritten: String if tagWritten == tag =>
      if (eventualConsistencyDelayMillis == 0)
        self ! Continue
      else
        context.system.scheduler.scheduleOnce(settings.eventualConsistencyDelay, self, Continue)
  }

  def timeForReplay: Boolean =
    !isToOffsetDone && (isBacktracking || buf.isEmpty || buf.size <= maxBufferSize / 2)

  def isToOffsetDone: Boolean = toOffset match {
    case None       => false
    case Some(uuid) => compare(currOffset, uuid) > 0
  }

  def isCurrentTimeAfterToOffset(): Boolean =
    toOffset match {
      case None    => false
      case Some(_) => System.currentTimeMillis() > toOffsetTimestamp
    }

  def stopIfDone(): Unit = {
    if (buf.isEmpty && (isToOffsetDone || isCurrentTimeAfterToOffset())) {
      onCompleteThenStop()
    }
  }

  def replay(): Unit = {
    val backtracking = isBacktracking
    val limit =
      if (backtracking) maxBufferSize
      else maxBufferSize - buf.size
    val toOffs =
      if (backtracking && abortDeadline.isEmpty) highestOffset
      else UUIDs.endOf(System.currentTimeMillis() - eventualConsistencyDelayMillis)
    if (log.isDebugEnabled)
      log.debug(
        s"${if (backtracking) "backtracking " else ""}query for tag [{}] from [{}] [{}] limit [{}]",
        tag, currTimeBucket, currOffset, limit
      )
    context.actorOf(EventsByTagFetcher.props(tag, currTimeBucket, currOffset, toOffs, limit, backtracking,
      self, preparedSelect, seqNumbers, settings))
    context.become(replaying(limit))
  }

  def replaying(limit: Int): Receive = {
    case env @ UUIDPersistentRepr(offs, _) =>
      currOffset = offs
      if (compare(currOffset, highestOffset) > 0)
        highestOffset = currOffset
      if (isToOffsetDone)
        stopIfDone()
      else
        buf :+= env
      deliverBuf()

    case ReplayDone(count, seqN, highest) =>
      log.debug("query chunk done for tag [{}], timBucket [{}], count [{}]", tag, currTimeBucket, count)
      seqNumbers = seqN
      currOffset = highest
      if (currOffset == highestOffset)
        abortDeadline = None // back on track again

      deliverBuf()

      if (count == 0) {
        if (isTimeBucketBeforeToday()) {
          nextTimeBucket()
          self ! Continue // more to fetch
        } else {
          stopIfDone()
        }
      } else {
        self ! Continue // more to fetch
      }

      context.become(idle)

    case ReplayAborted(seqN, pid, expectedSeqNr, gotSeqNr) =>
      // this will only happen when delayedEventTimeout is > 0s
      seqNumbers = seqN
      def logMsg = s"query chunk aborted for tag [$tag], timBucket [$currTimeBucket], " +
        s" expected sequence number [$expectedSeqNr] for [$pid], but got [$gotSeqNr]"
      abortDeadline match {
        case Some(deadline) if deadline.isOverdue =>
          val m = logMsg
          log.error(m)
          onErrorThenStop(new IllegalStateException(m))
        case _ =>
          if (log.isDebugEnabled) log.debug(logMsg)
          if (abortDeadline.isEmpty)
            abortDeadline = Some(Deadline.now + settings.delayedEventTimeout)
          // go back in history and look for missing sequence numbers
          goBack()
          context.become(idle)
      }

    case ReplayFailed(cause) =>
      log.debug("query failed for tag [{}], due to [{}]", tag, cause.getMessage)
      deliverBuf()
      onErrorThenStop(cause)

    case _: Request =>
      deliverBuf()

    case Continue => // skip during replay

    case Cancel =>
      context.stop(self)
  }

}
