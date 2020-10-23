/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded, Props, Timers }
import akka.annotation.InternalApi
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.pattern.pipe
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import akka.persistence.cassandra.journal.TagWriters.TagWritersSession

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.concurrent.duration._
import akka.actor.ReceiveTimeout
import akka.event.LoggingAdapter
import akka.util.{ OptionVal, UUIDComparator }

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
      pubsubNotification: Duration)

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

  private[akka] case class DropState(pid: PersistenceId)

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

  /**
   * Buffered events waiting to be written.
   * The next batch is maintained in `nextBatch` and will never contain more than the `batchSize`
   * or events from different time buckets.
   *
   * Events should be added and then call shouldWrite() to see if a batch is ready to be written.
   * Once a write is complete call `writeComplete` to discard the events in `nextBatch` and take
   * events from `pending` for the next batch.
   */
  case class Buffer(
      batchSize: Int,
      size: Int,
      nextBatch: Vector[AwaitingWrite],
      pending: Vector[AwaitingWrite],
      writeRequired: Boolean) {
    require(batchSize > 0)

    def isEmpty: Boolean = nextBatch.isEmpty

    def nonEmpty: Boolean = nextBatch.nonEmpty

    def remove(pid: String): Buffer = {
      val (toFilter, without) = nextBatch.partition(_.events.head._1.persistenceId == pid)
      val filteredPending = pending.filterNot(_.events.head._1.persistenceId == pid)
      val removed = toFilter.foldLeft(0)((acc, next) => acc + next.events.size)
      copy(size = size - removed, nextBatch = without, pending = filteredPending)
    }

    /**
     * Any time a new time bucket is received or the max batch size is reached then
     * a write should happen
     */
    def shouldWrite(): Boolean = {
      if (!writeRequired)
        require(size <= batchSize)
      writeRequired
    }

    final def add(write: AwaitingWrite): Buffer = {
      val firstTimeBucket = write.events.head._1.timeBucket
      val lastTimeBucket = write.events.last._1.timeBucket
      if (firstTimeBucket != lastTimeBucket) {
        // this write needs broken up as it spans multiple time buckets
        val (first, rest) = write.events.partition {
          case (serialized, _) => serialized.timeBucket == firstTimeBucket
        }
        add(AwaitingWrite(first, OptionVal.None)).add(AwaitingWrite(rest, write.ack))
      } else {
        // common case
        val newSize = size + write.events.size
        if (writeRequired) {
          // add them to pending, any time bucket changes will be detected later
          copy(size = newSize, pending = pending :+ write)
        } else if (nextBatch.headOption.exists(oldestEvent =>
                     UUIDComparator.comparator
                       .compare(write.events.head._1.timeUuid, oldestEvent.events.head._1.timeUuid) < 0)) {
          // rare case where events have been received out of order, just re-build the buffer
          require(pending.isEmpty)
          val allWrites = (nextBatch :+ write).sortBy(_.events.head._1.timeUuid)(timeUuidOrdering)
          rebuild(allWrites)
        } else if (nextBatch.headOption.exists(_.events.head._1.timeBucket != write.events.head._1.timeBucket)) {
          // time bucket has changed
          copy(size = newSize, pending = pending :+ write, writeRequired = true)
        } else if (newSize >= batchSize) {
          require(pending.isEmpty, "Pending should be empty if write not required")
          // does the new write need broken up?
          if (newSize > batchSize) {
            val toAdd = batchSize - size
            val (forNextWrite, forPending) = write.events.splitAt(toAdd)
            copy(
              size = newSize,
              nextBatch = nextBatch :+ AwaitingWrite(forNextWrite, OptionVal.None),
              pending = Vector(AwaitingWrite(forPending, write.ack)),
              writeRequired = true)
          } else {
            copy(size = newSize, nextBatch = nextBatch :+ write, writeRequired = true)
          }
        } else {
          copy(size = size + write.events.size, nextBatch = nextBatch :+ write)
        }
      }
    }

    private def rebuild(writes: Vector[AwaitingWrite]): Buffer = {
      var buffer = Buffer.empty(batchSize)
      var i = 0
      while (!buffer.shouldWrite() && i < writes.size) {
        buffer = buffer.add(writes(i))
        i += 1
      }
//       pending may have one in it as the last one may have been a time bucket change rather than bach full
      val done = buffer.copy(pending = buffer.pending ++ writes.drop(i))
      done
    }

    final def addPending(write: AwaitingWrite): Buffer = {
      copy(size = size + write.events.size, pending = pending :+ write)
    }

    def writeComplete(): Buffer = {
      // this could be more efficient by adding until a write is required but this is simpler and
      // pending is expected to be small unless the database is falling behind
      rebuild(pending)
    }
  }

  object Buffer {
    def empty(batchSize: Int): Buffer = {
      require(batchSize > 0)
      Buffer(batchSize, 0, Vector.empty, Vector.empty, writeRequired = false)
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
  import context.dispatcher

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

  override def preStart(): Unit = {
    log.debug("Running TagWriter for [{}] with settings {}", tag, settings)
    if (settings.stopTagWriterWhenIdle > Duration.Zero)
      context.setReceiveTimeout(settings.stopTagWriterWhenIdle)
  }

  override def receive: Receive =
    idle(Buffer.empty(settings.maxBatchSize), Map.empty[String, Long])

  private def idle(buffer: Buffer, tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr]): Receive = {
    case DropState(pid) =>
      log.debug("Dropping state for pid: {}", pid)
      context.become(idle(buffer, tagPidSequenceNrs - pid))
    case InternalFlush =>
      log.debug("Flushing")
      if (buffer.nonEmpty) {
        write(buffer, tagPidSequenceNrs, None)
      }
    case Flush =>
      if (buffer.nonEmpty) {
        // TODO this should br broken into batches https://github.com/akka/akka-persistence-cassandra/issues/405
        log.debug("External flush request from [{}]. Flushing.", sender())
        write(buffer, tagPidSequenceNrs, Some(sender()))
      } else {
        log.debug("External flush request from [{}], buffer empty.", sender())
        sender() ! FlushComplete
      }
    case TagWrite(_, payload, _) =>
      log.debug("Tag write {}", payload)
      val (newTagPidSequenceNrs, events: Seq[(Serialized, TagPidSequenceNr)]) = {
        assignTagPidSequenceNumbers(payload.toVector, tagPidSequenceNrs)
      }
      val newWrite = AwaitingWrite(events, OptionVal(sender()))
      val newBuffer = buffer.add(newWrite)
      flushIfRequired(newBuffer, newTagPidSequenceNrs)
    case twd: TagWriteDone =>
      log.error("Received Done when in idle state. This is a bug. Please report with DEBUG logs: {}", twd)
    case ResetPersistenceId(_, tp @ TagProgress(pid, _, tagPidSequenceNr)) =>
      log.debug("Resetting pid {}. TagProgress {}", pid, tp)
      become(idle(buffer.remove(pid), tagPidSequenceNrs + (pid -> tagPidSequenceNr)))
      sender() ! ResetPersistenceIdComplete

    case ReceiveTimeout =>
      if (buffer.isEmpty && tagPidSequenceNrs.isEmpty)
        parent ! PassivateTagWriter(tag)

    case StopTagWriter =>
      if (buffer.isEmpty && tagPidSequenceNrs.isEmpty)
        context.stop(self)
      else
        parent ! CancelPassivateTagWriter(tag)
  }

  private def writeInProgress(
      buffer: Buffer,
      tagPidSequenceNrs: Map[PersistenceId, TagPidSequenceNr],
      awaitingFlush: Option[ActorRef]): Receive = {
    case DropState(pid) =>
      log.debug("Dropping state for pid: [{}]", pid)
      // FIXME remove anything from the buffer
      become(writeInProgress(buffer, tagPidSequenceNrs - pid, awaitingFlush))
    case InternalFlush =>
    // Ignore, we will check when the write is done
    case Flush =>
      log.debug("External flush while write in progress. Will flush after write complete")
      become(writeInProgress(buffer, tagPidSequenceNrs, Some(sender())))
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
      val newBuffer = buffer.addPending(awaitingWrite)
      log.debug("buffering write as write in progress. New buffer {}", newBuffer)
      become(writeInProgress(newBuffer, updatedTagPidSequenceNrs, awaitingFlush))
    case TagWriteDone(summary, doneNotify) =>
      log.debug("Tag write done: {}", summary)
      val nextBuffer = buffer.writeComplete()
      buffer.nextBatch.foreach { write =>
        write.ack match {
          case OptionVal.None      =>
          case OptionVal.Some(ref) => ref ! Done
        }
      }
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
      awaitingFlush match {
        case Some(replyTo) =>
          log.debug("External flush request")
          if (buffer.pending.nonEmpty) {
            // TODO break into batches - check, but this is now done as buffer always batches
            write(nextBuffer, tagPidSequenceNrs, awaitingFlush)
          } else {
            replyTo ! FlushComplete
            context.become(idle(nextBuffer, tagPidSequenceNrs))
          }
        case None =>
          log.debug("write finished, checking if a new flush is required")
          flushIfRequired(nextBuffer, tagPidSequenceNrs)
      }
      sendPubsubNotification()
      doneNotify.foreach(_ ! FlushComplete)

    case TagWriteFailed(t, events) =>
      log.warning(
        "Writing tags has failed. This means that any eventsByTag query will be out of date. " +
        "The write will be retried. Reason {}",
        t)
      timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      parent ! TagWriters.TagWriteFailed(t)
      context.become(idle(buffer, tagPidSequenceNrs))

    case ResetPersistenceId(_, tp @ TagProgress(pid, _, _)) =>
      log.debug("Resetting persistence id {}. TagProgress {}", pid, tp)
      become(writeInProgress(buffer.remove(pid), tagPidSequenceNrs + (pid -> tp.pidTagSequenceNr), awaitingFlush))
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

  private def flushIfRequired(buffer: Buffer, tagSequenceNrs: Map[String, Long]): Unit = {
    if (buffer.isEmpty) {
      context.become(idle(buffer, tagSequenceNrs))
    } else if (buffer.shouldWrite() || settings.flushInterval == Duration.Zero) {
      write(buffer, tagSequenceNrs, None)
    } else {
      if (!timers.isTimerActive(FlushKey))
        timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      context.become(idle(buffer, tagSequenceNrs))
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
      buffer: Buffer,
      tagPidSequenceNrs: Map[String, TagPidSequenceNr],
      notifyWhenDone: Option[ActorRef]): Unit = {
    val writeSummary = createTagWriteSummary(buffer)
    log.debug("Starting tag write of {} events. Summary: {}", buffer.nextBatch.size, writeSummary)
    val withFailure = session.writeBatch(tag, buffer).map(_ => TagWriteDone(writeSummary, notifyWhenDone)).recover {
      case NonFatal(t) =>
        TagWriteFailed(t, buffer.nextBatch)
    }

    import context.dispatcher
    withFailure.pipeTo(self)

    // notifyWhenDone is cleared out as it is now in the TagWriteDone
    context.become(writeInProgress(buffer, tagPidSequenceNrs, None))
  }

  private def createTagWriteSummary(writes: Buffer): Map[PersistenceId, PidProgress] = {
    writes.nextBatch
      .flatten(_.events)
      .foldLeft(Map.empty[PersistenceId, PidProgress])((acc, next) => {
        val (event, tagPidSequenceNr) = next
        acc.get(event.persistenceId) match {
          case Some(PidProgress(from, to, _, _)) =>
            // FIXME make this log nicer
            if (event.sequenceNr <= to)
              throw new IllegalStateException(
                s"Expected events to be ordered by seqNr. ${event.persistenceId} " +
                s"Events: ${writes.nextBatch.map(e =>
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
}
