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
import java.time.format.DateTimeFormatter
import java.time.Instant

private[query] object EventsByTagPublisher {

  def props(tag: String, fromOffset: UUID, toOffset: Option[UUID], settings: CassandraReadJournalConfig,
            session: Session, preparedSelect: PreparedStatement): Props = {
    Props(classOf[EventsByTagPublisher], tag, fromOffset, toOffset,
      settings, PreparedStatementEnvelope(session, preparedSelect))
  }

  private[query] case object Continue extends DeadLetterSuppression

  private[query] case class ReplayDone(retrievedCount: Int, deliveredCount: Int, seqNumbers: Option[SequenceNumbers],
                                       highest: UUID, mightBeMore: Boolean)
    extends DeadLetterSuppression
  private[query] case class ReplayAborted(
    seqNumbers: Option[SequenceNumbers], persistenceId: String, expectedSeqNr: Option[Long], gotSeqNr: Long
  )
    extends DeadLetterSuppression
  private[query] final case class ReplayFailed(cause: Throwable)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def formatOffset(uuid: UUID): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(UUIDs.unixTimestamp(uuid)), ZoneOffset.UTC)
    s"$uuid (${timestampFormatter.format(time)})"
  }

  private[query] sealed trait BacktrackingMode extends NoSerializationVerificationNeeded
  private[query] case object NoBacktracking extends BacktrackingMode
  private[query] case object LookingForDelayed extends BacktrackingMode
  private[query] final case class LookingForMissing(
    toOffset: UUID, abortDeadLine: Deadline
  ) extends BacktrackingMode

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
  val lookForDelayedDuration: FiniteDuration = (settings.delayedEventTimeout / 2).min(20.seconds)
  var lookForDelayedDeadline: Deadline = nextLookForDelayedDeadline()
  private var backtrackingMode: BacktrackingMode = NoBacktracking
  var replayCountZero = false

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

  def backtracking: Boolean = backtrackingMode match {
    case NoBacktracking       => false
    case LookingForDelayed    => true
    case _: LookingForMissing => true
  }

  def goBack(lookForMissing: Boolean): Unit = {
    val timestamp = UUIDs.unixTimestamp(currOffset) - settings.delayedEventTimeout.toMillis
    val backFromOffset = UUIDs.startOf(timestamp)
    currOffset =
      if (compare(fromOffset, backFromOffset) >= 0) fromOffset
      else backFromOffset
    currTimeBucket = TimeBucket(currOffset)
    backtrackingMode match {
      case LookingForMissing(_, _) => // already in that mode
      case _ =>
        if (lookForMissing) {
          val toOffs = UUIDs.endOf(System.currentTimeMillis() - eventualConsistencyDelayMillis)
          backtrackingMode = LookingForMissing(toOffs, Deadline.now + settings.delayedEventTimeout)
        } else
          backtrackingMode = LookingForDelayed
    }
  }

  def nextLookForDelayedDeadline(): Deadline =
    Deadline.now + lookForDelayedDuration

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
      if (timeToLookForDelayed) {
        // look for delayed events
        goBack(lookForMissing = false)
        lookForDelayedDeadline = nextLookForDelayedDeadline()
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

  def timeToLookForDelayed: Boolean =
    strictBySeqNumber && !backtracking && lookForDelayedDeadline.isOverdue() && !isTimeBucketBeforeToday()

  def timeForReplay: Boolean =
    !isToOffsetDone && (backtracking || buf.isEmpty || buf.size <= maxBufferSize / 2)

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
    if (buf.isEmpty && (isToOffsetDone || (replayCountZero && isCurrentTimeAfterToOffset()))) {
      onCompleteThenStop()
    }
  }

  private def backtrackingLog: String = backtrackingMode match {
    case NoBacktracking       => ""
    case LookingForDelayed    => "backtracking delayed "
    case _: LookingForMissing => "backtracking missing "
  }

  def replay(): Unit = {
    val limit =
      if (backtracking) maxBufferSize
      else maxBufferSize - buf.size
    val toOffs = backtrackingMode match {
      case NoBacktracking               => UUIDs.endOf(System.currentTimeMillis() - eventualConsistencyDelayMillis)
      case LookingForDelayed            => highestOffset
      case LookingForMissing(toOffs, _) => toOffs
    }

    if (log.isDebugEnabled)
      log.debug(
        s"${backtrackingLog}query for tag [{}] from [{}] [{}] limit [{}]",
        tag, currTimeBucket, formatOffset(currOffset), limit
      )

    context.actorOf(EventsByTagFetcher.props(tag, currTimeBucket, currOffset, toOffs, limit,
      backtrackingMode, self, preparedSelect, seqNumbers, settings))
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

    case ReplayDone(retrievedCount, deliveredCount, seqN, highest, mightBeMore) =>
      if (log.isDebugEnabled)
        log.debug(
          s"${backtrackingLog}query chunk done for tag [{}], timBucket [{}], count [{}] of [{}]",
          tag, currTimeBucket, deliveredCount, retrievedCount
        )
      seqNumbers = seqN
      currOffset = highest
      replayCountZero = retrievedCount == 0

      deliverBuf()

      if (mightBeMore) {
        self ! Continue // fetched limit, more to fetch
      } else if (isTimeBucketBeforeToday()) {
        nextTimeBucket()
        self ! Continue // fetch more from next time bucket
      } else {
        if (backtrackingMode != NoBacktracking)
          currTimeBucket = TimeBucket(currOffset)
        backtrackingMode = NoBacktracking
        stopIfDone()
      }

      context.become(idle)

    case ReplayAborted(seqN, pid, expectedSeqNr, gotSeqNr) =>
      // this will only happen when delayedEventTimeout is > 0s
      seqNumbers = seqN
      def logMsg = expectedSeqNr match {
        case None => s"${backtrackingLog}query chunk aborted for tag [$tag], timBucket [$currTimeBucket], " +
          s" due to possibly first event for [$pid], got sequence number [$gotSeqNr]"
        case Some(expected) => s"${backtrackingLog}query chunk aborted for tag [$tag], timBucket [$currTimeBucket], " +
          s" due to missing event for [$pid], expected sequence number [$expected], but got [$gotSeqNr]"
      }
      backtrackingMode match {
        case LookingForMissing(_, deadline) if deadline.isOverdue =>
          val m = logMsg
          log.error(m)
          onErrorThenStop(new IllegalStateException(m))
        case _ =>
          if (log.isDebugEnabled) log.debug(logMsg)
          // go back in history and look for missing sequence numbers
          goBack(lookForMissing = true)
          context.become(idle)
          if (expectedSeqNr.isEmpty) {
            // start the backtracking query immediately when aborted due to possibly first event
            self ! Continue
          }
      }

    case ReplayFailed(cause) =>
      log.debug(s"${backtrackingLog}query failed for tag [{}], due to [{}]", tag, cause.getMessage)
      deliverBuf()
      onErrorThenStop(cause)

    case _: Request =>
      deliverBuf()

    case Continue => // skip during replay

    case Cancel =>
      context.stop(self)
  }

}
