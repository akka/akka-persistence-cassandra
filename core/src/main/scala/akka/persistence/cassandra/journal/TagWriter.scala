/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded, Props, ReceiveTimeout, Timers }
import akka.annotation.InternalApi
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.LoggingAdapter
import akka.pattern.pipe
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import akka.persistence.cassandra.journal.TagWriters.TagWritersSession
import akka.util.{ OptionVal, UUIDComparator }
import scala.concurrent.duration.{ Duration, FiniteDuration, _ }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/*
 * INTERNAL API
 *
 * Groups writes into un-logged batches for the same partition.
 *
 * For the read stage to work correctly events must be written in order for a given
 * persistence id for a given tag.
 *
 * Prevents any concurrent writes.
 *
 * Possible improvements:
 * - Max buffer size
 * - Optimize sorting given they are nearly sorted
 */
@InternalApi private[akka] object TagWriter {

  private[akka] def props(settings: TagWriterSettings, session: TagWritersSession, tag: Tag, parent: ActorRef): Props =
    Props(new TagWriter(settings, session, tag, parent))

  private[akka] case class TagWriterSettings(
      maxBatchSize: Int,
      flushInterval: FiniteDuration,
      scanningFlushInterval: FiniteDuration,
      stopTagWriterWhenIdle: FiniteDuration,
      pubsubNotification: Duration,
      retryMinBackoff: FiniteDuration,
      retryMaxBackoff: FiniteDuration)

  private[akka] case class TagProgress(
      persistenceId: PersistenceId,
      sequenceNr: SequenceNr,
      pidTagSequenceNr: TagPidSequenceNr)

  /*
   * Reset pid tag sequence numbers to the given [[TagProgress]] and discard any messages for the given persistenceId
   */
  private[akka] case class ResetPersistenceId(tag: Tag, progress: TagProgress) extends NoSerializationVerificationNeeded

  /*
   * Sent in response to a [[ResetPersistenceId]]
   */
  private[akka] case object ResetPersistenceIdComplete

  // Flush buffer regardless of size
  private[akka] case object Flush
  private[akka] case object FlushComplete

  // The "passivate pattern" is used to avoid loosing messages between TagWriters (parent)
  // and TagWriter when the TagWriter is stopped due to inactivity.
  // When idle for longer than configured stopTagWriterWhenIdle the TagWriter sends `PassivateTagWriter` to parent,
  // which replies with `StopTagWriter` and starts buffering incoming messages for the tag.
  // The TagWriter stops when receiving StopTagWriter if it is still ok to passivate (no state, nothing in progress).
  // TagWriters (parent) sends buffered messages if any when the TagWriter has been terminated.
  private[akka] final case class PassivateTagWriter(tag: String)
  private[akka] final case class CancelPassivateTagWriter(tag: String)
  private[akka] case object StopTagWriter

  type TagWriteSummary = Map[PersistenceId, PidProgress]
  case class PidProgress(seqNrFrom: SequenceNr, seqNrTo: SequenceNr, tagPidSequenceNr: TagPidSequenceNr, offset: UUID)
  private case object InternalFlush
  private case object FlushKey
  sealed trait TagWriteFinished
  final case class TagWriteDone(summary: TagWriteSummary, doneNotify: Option[ActorRef]) extends TagWriteFinished
  private final case class TagWriteFailed(reason: Throwable, failedEvents: Vector[AwaitingWrite])
      extends TagWriteFinished

  /**
   * Sent when a persistent actor terminates. This allows pending writes
   * to complete before cleaning up state, ensuring tag writes are not lost.
   */
  private[akka] case class PidTerminated(pid: PersistenceId)

  val timeUuidOrdering: Ordering[UUID] = new Ordering[UUID] {
    override def compare(x: UUID, y: UUID) =
      UUIDComparator.comparator.compare(x, y)
  }

  /**
   * The only reason ack is None is if a TagWrite needed to be broken up because the events were
   * from different time buckets or if a single write exceeds the batch size.
   * In that case the later AwaitingWrite contains the ack.
   */
  case class AwaitingWrite(events: Seq[(Serialized, TagPidSequenceNr)], ack: OptionVal[ActorRef])

  def writeBatch(
      buffer: Buffer,
      session: TagWritersSession,
      tag: String,
      writeSummary: TagWriteSummary,
      notifyWhenDone: Option[ActorRef])(implicit ec: ExecutionContext): Future[TagWriteFinished] = {
    session.writeBatch(tag, buffer).map(_ => TagWriteDone(writeSummary, notifyWhenDone)).recover {
      case NonFatal(t) =>
        TagWriteFailed(t, buffer.nextBatch)
    }
  }
}

/** INTERNAL API */
@InternalApi private[akka] class TagWriter(
    settings: TagWriterSettings,
    session: TagWritersSession,
    tag: String,
    parent: ActorRef)
    extends Actor
    with Timers
    with ActorLogging
    with NoSerializationVerificationNeeded {

  import TagWriter._
  import TagWriters.TagWrite
  import context.become

  // eager init and val because used from Future callbacks
  override val log: LoggingAdapter = super.log

  private val pubsub: Option[ActorRef] = {
    settings.pubsubNotification match {
      case interval: FiniteDuration =>
        Try {
          context.actorOf(PubSubThrottler.props(DistributedPubSub(context.system).mediator, interval))
        }.toOption
      case _ =>
        None
    }
  }

  var lastLoggedBufferNs: Long = -1
  val bufferWarningMinDurationNs: Long = 5.seconds.toNanos

  private var buffer: Buffer = Buffer.empty(settings.maxBatchSize)

  override def preStart(): Unit = {
    log.debug("Running TagWriter for [{}] with settings {}", tag, settings)
    if (settings.stopTagWriterWhenIdle > Duration.Zero)
      context.setReceiveTimeout(settings.stopTagWriterWhenIdle)
  }

  override def postStop(): Unit = {
    if (buffer.size > 0) {
      val pendingPidSeqNrs = (buffer.nextBatch.iterator ++ buffer.pending.iterator)
        .flatMap { w =>
          w.events.iterator.map { case (s, _) => s"${s.persistenceId}:${s.sequenceNr}" }
        }
        .mkString(", ")
      log.warning(
        "TagWriter for tag [{}] stopped with [{}] pending writes. " +
        "These events will be written when the persistent actor recovers. " +
        "Pending [{}].",
        tag,
        buffer.size,
        pendingPidSeqNrs)
    }
  }

  override def receive: Receive =
    idle(Map.empty[String, Long], Set.empty[PersistenceId], retryCount = 0)

  private def idle(
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr],
      pendingCleanup: Set[PersistenceId],
      retryCount: Int): Receive = {
    case PidTerminated(pid) =>
      if (bufferIsEmptyForPid(pid)) {
        // No pending writes, clean up immediately
        log.debug("Pid [{}] terminated with no pending writes, cleaning up state", pid)
        context.become(idle(tagPidSequenceNrs - pid, pendingCleanup - pid, retryCount))
      } else {
        // Mark for cleanup after writes complete
        log.debug("Pid [{}] terminated with pending writes, will cleanup after completion", pid)
        context.become(idle(tagPidSequenceNrs, pendingCleanup + pid, retryCount))
      }
    case InternalFlush =>
      log.debug("Flushing")
      if (buffer.nonEmpty) {
        write(tagPidSequenceNrs, pendingCleanup, retryCount, None)
      }
    case Flush =>
      if (buffer.nonEmpty) {
        log.debug("External flush request from [{}]. Flushing.", sender())
        write(tagPidSequenceNrs, pendingCleanup, retryCount, Some(sender()))
      } else {
        log.debug("External flush request from [{}], buffer empty.", sender())
        sender() ! FlushComplete
      }
    case TagWrite(_, payload, _) =>
      val (newTagPidSequenceNrs, events: Seq[(Serialized, TagPidSequenceNr)]) = {
        assignTagPidSequenceNumbers(payload.toVector, tagPidSequenceNrs)
      }
      val newWrite = AwaitingWrite(events, OptionVal(sender()))
      buffer = buffer.add(newWrite)
      flushIfRequired(newTagPidSequenceNrs, pendingCleanup, retryCount)
    case twd: TagWriteDone =>
      log.error("Received Done when in idle state. This is a bug. Please report with DEBUG logs: {}", twd)
    case ResetPersistenceId(_, tp @ TagProgress(pid, _, tagPidSequenceNr)) =>
      log.debug("Resetting pid {}. TagProgress {}", pid, tp)
      // Remove from pending cleanup - actor is alive again
      buffer = buffer.remove(pid)
      become(idle(tagPidSequenceNrs + (pid -> tagPidSequenceNr), pendingCleanup - pid, retryCount))
      sender() ! ResetPersistenceIdComplete

    case ReceiveTimeout =>
      if (buffer.isEmpty && tagPidSequenceNrs.isEmpty && pendingCleanup.isEmpty)
        parent ! PassivateTagWriter(tag)

    case StopTagWriter =>
      if (buffer.isEmpty && tagPidSequenceNrs.isEmpty && pendingCleanup.isEmpty)
        context.stop(self)
      else
        parent ! CancelPassivateTagWriter(tag)
  }

  private def writeInProgress(
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr],
      pendingCleanup: Set[PersistenceId],
      retryCount: Int,
      awaitingFlush: Option[ActorRef]): Receive = {
    case PidTerminated(pid) =>
      if (bufferIsEmptyForPid(pid)) {
        // No pending writes, clean up immediately
        log.debug("Pid [{}] terminated with no pending writes, cleaning up state", pid)
        become(writeInProgress(tagPidSequenceNrs - pid, pendingCleanup - pid, retryCount, awaitingFlush))
      } else {
        // Mark for cleanup after writes complete
        log.debug("Pid [{}] terminated with pending writes, will cleanup after completion", pid)
        become(writeInProgress(tagPidSequenceNrs, pendingCleanup + pid, retryCount, awaitingFlush))
      }
    case InternalFlush =>
    // Ignore, we will check when the write is done
    case Flush =>
      log.debug("External flush while write in progress. Will flush after write complete")
      become(writeInProgress(tagPidSequenceNrs, pendingCleanup, retryCount, Some(sender())))
    case TagWrite(_, payload, _) =>
      val (updatedTagPidSequenceNrs, events) =
        assignTagPidSequenceNumbers(payload.toVector, tagPidSequenceNrs)
      val awaitingWrite = AwaitingWrite(events, OptionVal(sender()))
      val now = System.nanoTime()
      if (buffer.size > (4 * settings.maxBatchSize) && now > (lastLoggedBufferNs + bufferWarningMinDurationNs)) {
        lastLoggedBufferNs = now
        log.warning(
          "Buffer for tagged events is getting too large ({}), is Cassandra responsive? Are writes failing? " +
          "If events are buffered for longer than the eventual-consistency-delay they won't be picked up by live queries. The oldest event in the buffer is offset: {}",
          buffer.size,
          formatOffset(buffer.nextBatch.head.events.head._1.timeUuid))
      }
      // buffer until current query is finished
      // Don't sort until the write has finished
      buffer = buffer.addPending(awaitingWrite)
      become(writeInProgress(updatedTagPidSequenceNrs, pendingCleanup, retryCount, awaitingFlush))
    case TagWriteDone(summary, doneNotify) =>
      log.debug("Tag write done: {}", summary)
      val completedBatch = buffer.nextBatch
      buffer = buffer.writeComplete()
      completedBatch.foreach { write =>
        write.ack match {
          case OptionVal.Some(ref) => ref ! Done
          case _                   =>
        }
      }
      summary.foreach {
        case (id, PidProgress(_, seqNrTo, tagPidSequenceNr, offset)) =>
          // These writes do not block future writes. We don't read the tag progress again from C*
          // until a restart has happened. This is best effort and expect recovery to replay any
          // events that aren't in the tag progress table
          import context.dispatcher
          session.writeProgress(tag, id, seqNrTo, tagPidSequenceNr, offset).onComplete {
            case Success(_) =>
            case Failure(t) =>
              log.warning(
                "Tag progress write has failed for pid: {} seqNrTo: {} tagPidSequenceNr: {} offset: {}. " +
                "If this is the only Cassandra error things will continue to work but if this keeps happening it will " +
                s" mean slower recovery as tag_views will need to be repaired. Reason: $t",
                id,
                seqNrTo,
                tagPidSequenceNr,
                formatOffset(offset))
              parent ! TagWriters.TagWriteFailed(t)
          }
      }
      // Check for pending cleanup - clean up pids that have terminated and have no more buffered events
      val updatedPendingCleanup = checkPendingCleanup(tagPidSequenceNrs, pendingCleanup)
      // Reset retry count on success
      awaitingFlush match {
        case Some(replyTo) =>
          log.debug("External flush request")
          if (buffer.nonEmpty) {
            write(updatedPendingCleanup._1, updatedPendingCleanup._2, retryCount = 0, awaitingFlush)
          } else {
            replyTo ! FlushComplete
            context.become(idle(updatedPendingCleanup._1, updatedPendingCleanup._2, retryCount = 0))
          }
        case None =>
          flushIfRequired(updatedPendingCleanup._1, updatedPendingCleanup._2, retryCount = 0)
      }
      sendPubsubNotification()
      doneNotify.foreach(_ ! FlushComplete)

    case TagWriteFailed(t, _) =>
      val newRetryCount = retryCount + 1
      val backoffDelay = calculateBackoff(newRetryCount)
      log.warning(
        "Writing tags has failed (attempt {}). This means that any eventsByTag query will be out of date. " +
        "The write will be retried in {}. Reason: {}",
        newRetryCount,
        backoffDelay,
        t)
      timers.startSingleTimer(FlushKey, InternalFlush, backoffDelay)
      parent ! TagWriters.TagWriteFailed(t)
      context.become(idle(tagPidSequenceNrs, pendingCleanup, newRetryCount))

    case ResetPersistenceId(_, tp @ TagProgress(pid, _, _)) =>
      log.debug("Resetting persistence id {}. TagProgress {}", pid, tp)
      // Remove from pending cleanup - actor is alive again
      buffer = buffer.remove(pid)
      become(
        writeInProgress(
          tagPidSequenceNrs + (pid -> tp.pidTagSequenceNr),
          pendingCleanup - pid,
          retryCount,
          awaitingFlush))
      sender() ! ResetPersistenceIdComplete

    case ReceiveTimeout =>
    // not idle

    case StopTagWriter =>
      // not idle any more
      parent ! CancelPassivateTagWriter(tag)
  }

  private def sendPubsubNotification(): Unit = {
    pubsub.foreach {
      _ ! DistributedPubSubMediator.Publish(s"apc.tags.$tag", tag)
    }
  }

  private def flushIfRequired(
      tagSequenceNrs: Map[String, Long],
      pendingCleanup: Set[PersistenceId],
      retryCount: Int): Unit = {
    if (buffer.isEmpty) {
      context.become(idle(tagSequenceNrs, pendingCleanup, retryCount))
    } else if (buffer.shouldWrite() || settings.flushInterval == Duration.Zero) {
      write(tagSequenceNrs, pendingCleanup, retryCount, None)
    } else {
      if (!timers.isTimerActive(FlushKey))
        timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      context.become(idle(tagSequenceNrs, pendingCleanup, retryCount))
    }
  }

  /**
   * Defaults to 1 as if recovery for a persistent Actor based its recovery
   * on anything other than no progress then it sends a msg to the tag writer.
   */
  private def calculateTagPidSequenceNr(
      pid: PersistenceId,
      tagPidSequenceNrs: Map[String, TagPidSequenceNr]): (Map[String, TagPidSequenceNr], TagPidSequenceNr) = {
    val tagPidSequenceNr = tagPidSequenceNrs.get(pid) match {
      case None =>
        1L
      case Some(n) =>
        n + 1
    }
    (tagPidSequenceNrs + (pid -> tagPidSequenceNr), tagPidSequenceNr)
  }

  /**
   * Events should be ordered by sequence nr per pid
   */
  private def write(
      tagPidSequenceNrs: Map[String, TagPidSequenceNr],
      pendingCleanup: Set[PersistenceId],
      retryCount: Int,
      notifyWhenDone: Option[ActorRef]): Unit = {
    val writeSummary = createTagWriteSummary()
    log.debug("Starting tag write of {} events. Summary: {}", buffer.nextBatch.size, writeSummary)
    import context.dispatcher
    val withFailure = TagWriter.writeBatch(buffer, session, tag, writeSummary, notifyWhenDone)
    withFailure.pipeTo(self)

    // notifyWhenDone is cleared out as it is now in the TagWriteDone
    context.become(writeInProgress(tagPidSequenceNrs, pendingCleanup, retryCount, None))
  }

  /**
   * Calculate exponential backoff delay for retries.
   * `delay = min(maxBackoff, minBackoff * 2^retryCount)`
   */
  private def calculateBackoff(retryCount: Int): FiniteDuration = {
    val minMs = settings.retryMinBackoff.toMillis
    val maxMs = settings.retryMaxBackoff.toMillis
    val delayMs = math.min(maxMs, minMs * math.pow(2, retryCount - 1).toLong)
    delayMs.millis
  }

  private def createTagWriteSummary(): Map[PersistenceId, PidProgress] = {
    buffer.nextBatch
      .flatten(_.events)
      .foldLeft(Map.empty[PersistenceId, PidProgress])((acc, next) => {
        val (event, tagPidSequenceNr) = next
        acc.get(event.persistenceId) match {
          case Some(PidProgress(from, to, _, _)) =>
            if (event.sequenceNr <= to)
              throw new IllegalStateException(
                s"Expected events to be ordered by seqNr. ${event.persistenceId} " +
                s"Events: ${buffer.nextBatch.map(e =>
                  (e.events.head._1.persistenceId, e.events.head._1.sequenceNr, e.events.head._1.timeUuid))}")

            acc + (event.persistenceId -> PidProgress(from, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
          case None =>
            acc + (event.persistenceId -> PidProgress(
              event.sequenceNr,
              event.sequenceNr,
              tagPidSequenceNr,
              event.timeUuid))
        }
      })
  }

  private def assignTagPidSequenceNumbers(
      events: Vector[Serialized],
      currentTagPidSequenceNrs: Map[String, TagPidSequenceNr])
      : (Map[String, TagPidSequenceNr], Seq[(Serialized, TagPidSequenceNr)]) =
    events.foldLeft((currentTagPidSequenceNrs, Vector.empty[(Serialized, TagPidSequenceNr)])) {
      case ((accTagSequenceNrs, accEvents), nextEvent) =>
        val (newSequenceNrs, sequenceNr: TagPidSequenceNr) =
          calculateTagPidSequenceNr(nextEvent.persistenceId, accTagSequenceNrs)
        (newSequenceNrs, accEvents :+ ((nextEvent, sequenceNr)))
    }

  /**
   * Check if a persistence id has no buffered events (neither in nextBatch nor in pending).
   */
  private def bufferIsEmptyForPid(pid: PersistenceId): Boolean = {
    !buffer.nextBatch.exists(_.events.exists(_._1.persistenceId == pid)) &&
    !buffer.pending.exists(_.events.exists(_._1.persistenceId == pid))
  }

  /**
   * Check for pending cleanup and clean up pids that have terminated and have no more buffered events.
   * Returns the updated (tagPidSequenceNrs, pendingCleanup).
   */
  private def checkPendingCleanup(
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr],
      pendingCleanup: Set[PersistenceId]): (Map[PersistenceId, TagPidSequenceNr], Set[PersistenceId]) = {
    pendingCleanup.foldLeft((tagPidSequenceNrs, pendingCleanup)) {
      case ((accSeqNrs, accPending), pid) =>
        if (bufferIsEmptyForPid(pid)) {
          log.debug("Cleaned up state for terminated pid [{}]", pid)
          (accSeqNrs - pid, accPending - pid)
        } else {
          (accSeqNrs, accPending)
        }
    }
  }
}
