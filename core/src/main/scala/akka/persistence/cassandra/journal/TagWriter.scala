/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded, Props, Timers }
import akka.annotation.InternalApi
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.pattern.pipe
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import akka.persistence.cassandra.journal.TagWriters.TagWritersSession
import akka.persistence.cassandra.query.UUIDComparator

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.concurrent.duration._

/**
 * INTERNAL API
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

  @InternalApi private[akka] def props(settings: TagWriterSettings, session: TagWritersSession, tag: Tag): Props =
    Props(new TagWriter(settings, session, tag))

  @InternalApi private[akka] case class TagWriterSettings(
      maxBatchSize: Int,
      flushInterval: FiniteDuration,
      scanningFlushInterval: FiniteDuration,
      pubsubNotification: Boolean)

  @InternalApi private[akka] case class TagProgress(
      persistenceId: PersistenceId,
      sequenceNr: SequenceNr,
      pidTagSequenceNr: TagPidSequenceNr)

  /**
   * INTERNAL API
   * Reset pid tag sequence numbers to the given [[TagProgress]] and discard any messages for the given persistenceId.
   */
  @InternalApi private[akka] case class ResetPersistenceId(tag: Tag, progress: TagProgress) extends NoSerializationVerificationNeeded

  /**
   * INTERNAL APIa
   * Sent in response to a [[ResetPersistenceId]].
   */
  @InternalApi private[akka] case object ResetPersistenceIdComplete

  // Flush buffer regardless of size
  @InternalApi private[akka] case object Flush
  @InternalApi private[akka] case object FlushComplete

  type TagWriteSummary = Map[PersistenceId, PidProgress]
  case class PidProgress(seqNrFrom: SequenceNr, seqNrTo: SequenceNr, tagPidSequenceNr: TagPidSequenceNr, offset: UUID)
  private case object InternalFlush
  private case object FlushKey
  sealed trait TagWriteFinished
  final case class TagWriteDone(summary: TagWriteSummary, doneNotify: Option[ActorRef]) extends TagWriteFinished
  private final case class TagWriteFailed(reason: Throwable, failedEvents: Vector[(Serialized, TagPidSequenceNr)])
      extends TagWriteFinished

  @InternalApi private[akka] case class DropState(pid: PersistenceId)

  val timeUuidOrdering = new Ordering[UUID] {
    override def compare(x: UUID, y: UUID) =
      UUIDComparator.comparator.compare(x, y)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TagWriter(settings: TagWriterSettings, session: TagWritersSession, tag: String)
    extends Actor
    with Timers
    with ActorLogging
    with NoSerializationVerificationNeeded {

  import TagWriter._
  import TagWriters.TagWrite
  import context.become
  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log = super.log

  private val pubsub: Option[DistributedPubSub] =
    if (settings.pubsubNotification) {
      Try {
        DistributedPubSub(context.system)
      }.toOption
    } else {
      None
    }

  private val parent = context.parent

  var lastLoggedBufferNs: Long = -1
  val bufferWarningMinDurationNs: Long = 5.seconds.toNanos

  override def preStart(): Unit =
    log.debug("Running TagWriter for [{}] with settings {}", tag, settings)

  override def receive: Receive =
    idle(Vector.empty[(Serialized, TagPidSequenceNr)], Map.empty[String, Long])

  private def idle(
      buffer: Vector[(Serialized, TagPidSequenceNr)],
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr]): Receive = {
    case DropState(pid) =>
      log.debug("Dropping state for pid: {}", pid)
      context.become(idle(buffer, tagPidSequenceNrs - pid))
    case InternalFlush =>
      log.debug("Flushing")
      if (buffer.nonEmpty) {
        write(buffer.take(settings.maxBatchSize), buffer.drop(settings.maxBatchSize), tagPidSequenceNrs, None)
      }
    case Flush =>
      if (buffer.nonEmpty) {
        // FIXME, this should br broken into batches https://github.com/akka/akka-persistence-cassandra/issues/405
        log.debug("External flush request from [{}]. Flushing.", sender())
        write(buffer, Vector.empty[(Serialized, TagPidSequenceNr)], tagPidSequenceNrs, Some(sender()))
      } else {
        log.debug("External flush request from [{}], buffer empty.", sender())
        sender() ! FlushComplete
      }
    case TagWrite(_, payload, _) =>
      // FIXME, keeping this sorted is over kill. We only need to know if a new timebucket is
      // reached to force a flush or that the batch size is met
      val (newTagPidSequenceNrs, events) =
        assignTagPidSequenceNumbers(payload.toVector, tagPidSequenceNrs)
      log.debug("Assigned tag pid sequence nrs: {}", events)
      val newBuffer = (buffer ++ events).sortBy(_._1.timeUuid)(timeUuidOrdering)
      flushIfRequired(newBuffer, newTagPidSequenceNrs)
    case twd: TagWriteDone =>
      log.error("Received Done when in idle state. This is a bug. Please report with DEBUG logs: {}", twd)
    case ResetPersistenceId(_, tp @ TagProgress(pid, _, tagPidSequenceNr)) =>
      log.debug("Resetting pid {}. TagProgress {}", pid, tp)
      become(idle(buffer.filterNot(_._1.persistenceId == pid), tagPidSequenceNrs + (pid -> tagPidSequenceNr)))
      sender() ! ResetPersistenceIdComplete
  }

  private def writeInProgress(
      buffer: Vector[(Serialized, TagPidSequenceNr)],
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr],
      awaitingFlush: Option[ActorRef]): Receive = {
    case DropState(pid) =>
      log.debug("Dropping state for pid: [{}]", pid)
      become(writeInProgress(buffer, tagPidSequenceNrs - pid, awaitingFlush))
    case InternalFlush =>
    // Ignore, we will check when the write is done
    case Flush =>
      log.debug("External flush while write in progress. Will flush after write complete")
      become(writeInProgress(buffer, tagPidSequenceNrs, Some(sender())))
    case TagWrite(_, payload, _) =>
      val (updatedTagPidSequenceNrs, events) =
        assignTagPidSequenceNumbers(payload.toVector, tagPidSequenceNrs)
      val now = System.nanoTime()
      if (buffer.size > (4 * settings.maxBatchSize) && now > (lastLoggedBufferNs + bufferWarningMinDurationNs)) {
        lastLoggedBufferNs = now
        log.warning(
          "Buffer for tagged events is getting too large ({}), is Cassandra responsive? Are writes failing? " +
          "If events are buffered for longer than the eventual-consistency-delay they won't be picked up by live queries. The oldest event in the buffer is offset: {}",
          buffer.size,
          formatOffset(buffer.head._1.timeUuid))
      }
      // buffer until current query is finished
      // Don't sort until the write has finished
      become(writeInProgress(buffer ++ events, updatedTagPidSequenceNrs, awaitingFlush))
    case TagWriteDone(summary, doneNotify) =>
      val sortedBuffer = buffer.sortBy(_._1.timeUuid)(timeUuidOrdering)
      log.debug("Tag write done: {}", summary)
      summary.foreach {
        case (id, PidProgress(_, seqNrTo, tagPidSequenceNr, offset)) =>
          // These writes do not block future writes. We don't read the tag progress again from C*
          // until a restart has happened. This is best effort and expect recovery to replay any
          // events that aren't in the tag progress table
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
      log.debug(s"Tag write complete. {}", summary)
      awaitingFlush match {
        case Some(replyTo) =>
          log.debug("External flush request")
          if (sortedBuffer.nonEmpty) {
            // FIXME, break into batches
            write(sortedBuffer, Vector.empty[(Serialized, TagPidSequenceNr)], tagPidSequenceNrs, awaitingFlush)
          } else {
            replyTo ! FlushComplete
            context.become(idle(sortedBuffer, tagPidSequenceNrs))
          }
        case None =>
          flushIfRequired(sortedBuffer, tagPidSequenceNrs)
      }
      pubsub.foreach {
        _.mediator ! DistributedPubSubMediator.Publish("akka.persistence.cassandra.journal.tag", tag)
      }
      doneNotify.foreach(_ ! FlushComplete)

    case TagWriteFailed(t, events) =>
      log.warning(
        "Writing tags has failed. This means that any eventsByTag query will be out of date. " +
        "The write will be retried. Reason {}",
        t)
      timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      parent ! TagWriters.TagWriteFailed(t)
      context.become(idle(events ++ buffer, tagPidSequenceNrs))

    case ResetPersistenceId(_, tp @ TagProgress(pid, _, _)) =>
      log.debug("Resetting persistence id {}. TagProgress {}", pid, tp)
      become(
        writeInProgress(
          buffer.filterNot(_._1.persistenceId == pid),
          tagPidSequenceNrs + (pid -> tp.pidTagSequenceNr),
          awaitingFlush))
      sender() ! ResetPersistenceIdComplete
  }

  private def flushIfRequired(buffer: Vector[(Serialized, TagPidSequenceNr)], tagSequenceNrs: Map[String, Long]): Unit =
    if (buffer.isEmpty) {
      context.become(idle(buffer, tagSequenceNrs))
    } else if (buffer.head._1.timeBucket < buffer.last._1.timeBucket) {
      val (currentBucket, rest) =
        buffer.span(_._1.timeBucket == buffer.head._1.timeBucket)
      if (log.isDebugEnabled) {
        log.debug(
          "Switching time buckets: head: {} last: {}. Number in current bucket: {}",
          buffer.head._1.timeBucket,
          buffer.last._1.timeBucket,
          currentBucket.size)
      }

      if (currentBucket.size > settings.maxBatchSize) {
        write(buffer.take(settings.maxBatchSize), buffer.drop(settings.maxBatchSize), tagSequenceNrs, None)
      } else {
        write(currentBucket, rest, tagSequenceNrs, None)
      }
    } else if (buffer.size >= settings.maxBatchSize) {
      log.debug("Batch size reached. Writing to Cassandra.")
      write(buffer.take(settings.maxBatchSize), buffer.drop(settings.maxBatchSize), tagSequenceNrs, None)
    } else if (settings.flushInterval == Duration.Zero) {
      log.debug("Flushing right away as interval is zero")
      // Should always be a buffer of 1
      write(buffer, Vector.empty[(Serialized, TagPidSequenceNr)], tagSequenceNrs, None)
    } else {
      timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      context.become(idle(buffer, tagSequenceNrs))
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
      events: Vector[(Serialized, TagPidSequenceNr)],
      remainingBuffer: Vector[(Serialized, TagPidSequenceNr)],
      tagPidSequenceNrs: Map[String, TagPidSequenceNr],
      notifyWhenDone: Option[ActorRef]): Unit = {
    val writeSummary = createTagWriteSummary(events)
    log.debug("Starting tag write of {} events. Summary: {}", events.size, writeSummary)
    val withFailure = session.writeBatch(tag, events).map(_ => TagWriteDone(writeSummary, notifyWhenDone)).recover {
      case NonFatal(t) =>
        TagWriteFailed(t, events)
    }

    import context.dispatcher
    withFailure.pipeTo(self)

    // notifyWhenDone is cleared out as it is now in the TagWriteDone
    context.become(writeInProgress(remainingBuffer, tagPidSequenceNrs, None))
  }

  private def createTagWriteSummary(writes: Seq[(Serialized, TagPidSequenceNr)]): Map[PersistenceId, PidProgress] =
    writes.foldLeft(Map.empty[PersistenceId, PidProgress])((acc, next) => {
      val (event, tagPidSequenceNr) = next
      acc.get(event.persistenceId) match {
        case Some(PidProgress(from, to, _, _)) =>
          if (event.sequenceNr <= to)
            throw new IllegalStateException(
              s"Expected events to be ordered by seqNr. ${event.persistenceId} " +
              s"Events: ${writes.map(e => (e._1.persistenceId, e._1.sequenceNr, e._1.timeUuid))}")
          acc + (event.persistenceId -> PidProgress(from, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
        case None =>
          acc + (event.persistenceId -> PidProgress(
            event.sequenceNr,
            event.sequenceNr,
            tagPidSequenceNr,
            event.timeUuid))
      }
    })

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
}
