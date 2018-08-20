/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
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
import scala.util.{ Failure, Success, Try }

/*
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

  private[akka] def props(
    settings: TagWriterSettings,
    session:  TagWritersSession,
    tag:      Tag): Props = Props(new TagWriter(settings, session, tag))

  private[akka] case class TagWriterSettings(
    maxBatchSize:          Int,
    flushInterval:         FiniteDuration,
    scanningFlushInterval: FiniteDuration,
    pubsubNotification:    Boolean)

  private[akka] case class TagProgress(
    persistenceId:    PersistenceId,
    sequenceNr:       SequenceNr,
    pidTagSequenceNr: TagPidSequenceNr)

  /*
   * Reset pid tag sequence numbers to the given [[TagProgress]] and discard any messages for the given persistenceId
   */
  private[akka] case class ResetPersistenceId(tag: Tag, progress: TagProgress)
    extends NoSerializationVerificationNeeded

  /*
   * Sent in response to a [[ResetPersistenceId]]
   */
  private[akka] case object ResetPersistenceIdComplete

  // Flush buffer regardless of size
  private[akka] case object Flush
  private[akka] case object FlushComplete

  type TagWriteSummary = Map[PersistenceId, PidProgress]
  case class PidProgress(seqNrFrom: SequenceNr, seqNrTo: SequenceNr, tagPidSequenceNr: TagPidSequenceNr, offset: UUID)
  private case object InternalFlush
  private case object FlushKey
  sealed trait TagWriteFinished
  final case class TagWriteDone(summary: TagWriteSummary, doneNotify: Option[ActorRef]) extends TagWriteFinished
  private final case class TagWriteFailed(reason: Throwable, failedEvents: Vector[Serialized], previousTagSequenceNrs: Map[String, Long]) extends TagWriteFinished

  val timeUuidOrdering = new Ordering[UUID] {
    override def compare(x: UUID, y: UUID) = UUIDComparator.comparator.compare(x, y)
  }
}

@InternalApi private[akka] class TagWriter(settings: TagWriterSettings, session: TagWritersSession, tag: String)
  extends Actor with Timers with ActorLogging with NoSerializationVerificationNeeded {

  import TagWriter._
  import TagWriters.TagWrite
  import context.become
  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log = super.log

  log.debug("Running TagWriter for [{}] with settings {}", tag, settings)

  private val pubsub: Option[DistributedPubSub] = if (settings.pubsubNotification) {
    Try {
      DistributedPubSub(context.system)
    }.toOption
  } else {
    None
  }

  private val parent = context.parent

  // TODO remove once stable, or make part of the state in stack rather than a var
  var sequenceNrs = Map.empty[String, Long]

  override def receive: Receive = idle(Vector.empty[Serialized], Map.empty[String, Long])

  def validate(payload: immutable.Seq[Serialized], buffer: Vector[Serialized]): Unit = {
    payload.foreach { s =>
      sequenceNrs.get(s.persistenceId) match {
        case Some(seqNr) =>
          if (seqNr >= s.sequenceNr) {
            val currentBuffer = buffer.map(s => (s.persistenceId, s.sequenceNr, formatOffset(s.timeUuid)))
            val newEvents = buffer.map(s => (s.persistenceId, s.sequenceNr, formatOffset(s.timeUuid)))
            log.error(
              "Received sequence numbers out of order. Pid: {}. HighestSeq: {}. New events: {}. Buffered events: {}",
              s.persistenceId, seqNr, newEvents, currentBuffer)
            throw new IllegalStateException(s"Not received sequence numbers in order for pid: ${s.persistenceId}, " +
              s"current highest sequenceNr: ${sequenceNrs(s.persistenceId)}. Sequence nr in event: ${seqNr}")
          }
          sequenceNrs += (s.persistenceId -> s.sequenceNr)
        case None =>
          sequenceNrs += (s.persistenceId -> s.sequenceNr)
      }
    }

    // FIXME remove this logging once stable, too verbose
    if (log.isDebugEnabled) {
      val pidsUpdated = payload.map(_.persistenceId).toSet
      pidsUpdated.foreach(p => {
        log.debug("Updated largest sequence nr for pid {} to {}", p, sequenceNrs(p))
      })
    }
  }

  private def idle(buffer: Vector[Serialized], tagPidSequenceNrs: Map[String, Long]): Receive = {
    case InternalFlush =>
      log.debug("Flushing")
      if (buffer.nonEmpty) {
        write(buffer, Vector.empty[Serialized], tagPidSequenceNrs, None)
      }
    case Flush =>
      if (buffer.nonEmpty) {
        log.debug("External flush request. Flushing.")
        write(buffer, Vector.empty[Serialized], tagPidSequenceNrs, Some(sender()))
      } else {
        log.debug("External flush request, buffer empty.")
        sender() ! FlushComplete
      }
    case TagWrite(_, payload) =>
      validate(payload, buffer)
      flushIfRequired((buffer ++ payload).sortBy(_.timeUuid)(timeUuidOrdering), tagPidSequenceNrs)
    case twd: TagWriteDone =>
      log.error("Received Done when in idle state. This is a bug. Please report with DEBUG logs: {}", twd)
    case ResetPersistenceId(_, tp @ TagProgress(pid, sequenceNr, tagPidSequenceNr)) =>
      log.debug("Resetting persistence id {}. TagProgress {}", pid, tp)
      sequenceNrs += (tp.persistenceId -> sequenceNr)
      become(idle(buffer.filterNot(_.persistenceId == pid), tagPidSequenceNrs + (pid -> tagPidSequenceNr)))
      sender() ! ResetPersistenceIdComplete
  }

  private def writeInProgress(
    buffer:             Vector[Serialized],
    tagPidSequenceNrs:  Map[String, Long],
    updatedTagProgress: Map[String, Long],
    awaitingFlush:      Option[ActorRef]   = None): Receive = {
    case InternalFlush =>
    // Ignore, we will check when the write is done
    case Flush =>
      log.debug("External flush while write in progress. Will flush after write complete")
      become(writeInProgress(buffer, tagPidSequenceNrs, updatedTagProgress, Some(sender())))
    case TagWrite(_, payload) =>
      validate(payload, buffer)
      // buffer until current query is finished
      become(writeInProgress((buffer ++ payload).sortBy(_.timeUuid)(timeUuidOrdering), tagPidSequenceNrs, updatedTagProgress))
    case TagWriteDone(summary, doneNotify) =>
      summary.foreach {
        case (id, p @ PidProgress(_, seqNrTo, tagPidSequenceNr, offset)) =>
          log.debug("Writing tag progress {}", p)
          // These writes do not block future writes. We don't read he tag progress again from C*
          // until a restart has happened. This is best effort and expect recovery to replay any
          // events that aren't in the tag progress table
          session.writeProgress(tag, id, seqNrTo, tagPidSequenceNr, offset).onComplete {
            case Success(_) =>
              log.debug(
                "Tag progress written. Pid: {} SeqNrTo: {} tagPidSequenceNr: {} offset: {}",
                id, seqNrTo, tagPidSequenceNr, offset)
            case Failure(t) =>
              log.warning(
                "Tag progress write has failed for pid: {} seqNrTo: {} tagPidSequenceNr: {} offset: {}. " +
                  "If this is the only Cassandra error things will continue to work but if this keeps happening it till " +
                  s" mean slower recovery as tag_views will need to be repaired. Reason: $t",
                id, seqNrTo, tagPidSequenceNr, formatOffset(offset))
              parent ! TagWriters.TagWriteFailed(t)
          }
      }
      log.debug(s"Tag write complete. {}", summary)
      if (awaitingFlush.isDefined) {
        log.debug("External flush request")
        if (buffer.nonEmpty) {
          write(buffer, Vector.empty[Serialized], tagPidSequenceNrs ++ updatedTagProgress, awaitingFlush)
        } else {
          sender() ! FlushComplete
          context.become(idle(buffer, tagPidSequenceNrs ++ updatedTagProgress))
        }
      } else {
        flushIfRequired(buffer, tagPidSequenceNrs ++ updatedTagProgress)
      }
      pubsub.foreach {
        log.debug("Publishing tag update for {}", tag)
        _.mediator ! DistributedPubSubMediator.Publish("akka.persistence.cassandra.journal.tag", tag)
      }
      doneNotify.foreach(_ ! FlushComplete)

    case TagWriteFailed(t, events, previousTagPidSequenceNrs) =>
      log.warning("Writing tags has failed. This means that any eventsByTag query will be out of date. " +
        "The write will be retried. Reason {}", t)
      log.debug("Setting tag sequence nrs back to {}", previousTagPidSequenceNrs)
      timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      parent ! TagWriters.TagWriteFailed(t)
      context.become(idle(events ++ buffer, previousTagPidSequenceNrs ++ updatedTagProgress))

    case ResetPersistenceId(_, tp @ TagProgress(pid, _, _)) =>
      log.debug("Resetting persistence id {}. TagProgress {}", pid, tp)
      sequenceNrs += (pid -> tp.sequenceNr)
      become(writeInProgress(buffer.filterNot(_.persistenceId == pid), tagPidSequenceNrs, updatedTagProgress + (pid -> tp.pidTagSequenceNr)))
      sender() ! ResetPersistenceIdComplete
  }

  private def flushIfRequired(buffer: Vector[Serialized], tagSequenceNrs: Map[String, Long]): Unit = {
    if (buffer.isEmpty) {
      context.become(idle(buffer, tagSequenceNrs))
    } else if (buffer.head.timeBucket < buffer.last.timeBucket) {
      val (currentBucket, rest) = buffer.span(_.timeBucket == buffer.head.timeBucket)
      if (log.isDebugEnabled) {
        log.debug("Switching time buckets: head: {} last: {}", buffer.head.timeBucket, buffer.last.timeBucket)
      }
      write(currentBucket, rest, tagSequenceNrs)
    } else if (buffer.size >= settings.maxBatchSize) {
      log.debug("Batch size reached. Writing to Cassandra.")
      write(buffer.take(settings.maxBatchSize), buffer.drop(settings.maxBatchSize), tagSequenceNrs)
    } else if (settings.flushInterval == Duration.Zero) {
      log.debug("Flushing right away as interval is zero")
      write(buffer, Vector.empty[Serialized], tagSequenceNrs)
    } else {
      timers.startSingleTimer(FlushKey, InternalFlush, settings.flushInterval)
      // FIXME, remove the time buckets
      if (log.isDebugEnabled) {
        log.debug(
          "Batch size not reached, buffering. Current buffer size: {}. First timebucket: {} Last timebucket: {}",
          buffer.size, buffer.head.timeBucket, buffer.last.timeBucket)
      }
      context.become(idle(buffer, tagSequenceNrs))
    }
  }

  /**
   * Defaults to 1 as if recovery for a persistent Actor based its recovery
   * on anything other than no progress then it sends a msg to the tag writer.
   * FIXME: Remove logging once stable
   */
  private def calculateTagPidSequenceNr(pid: PersistenceId, tagPidSequenceNrs: Map[String, TagPidSequenceNr]): (Map[String, TagPidSequenceNr], TagPidSequenceNr) = {
    val tagPidSequenceNr = tagPidSequenceNrs.get(pid) match {
      case None =>
        log.debug("First time seeing this pId {} since startup. Setting it to 1", pid)
        1L
      case Some(n) =>
        log.debug("Pid {} Already cached. Previous tagPidSequenceNr: {}", pid, n)
        n + 1
    }
    (tagPidSequenceNrs + (pid -> tagPidSequenceNr), tagPidSequenceNr)
  }

  /**
   * Events should be ordered by sequence nr per pid
   */
  private def write(
    events:                 Vector[Serialized],
    remainingBuffer:        Vector[Serialized],
    existingTagSequenceNrs: Map[Tag, TagPidSequenceNr],
    notifyWhenDone:         Option[ActorRef]           = None): Unit = {
    val (newTagSequenceNrs, withPidTagSeqNr) = assignTagPidSequenceNumbers(events, existingTagSequenceNrs)
    val writeSummary = createTagWriteSummary(withPidTagSeqNr, events)
    log.debug("Starting tag write. Summary: {}", writeSummary)
    val withFailure = session.writeBatch(tag, withPidTagSeqNr)
      .map(_ => TagWriteDone(writeSummary, notifyWhenDone))
      .recover {
        case t: Throwable =>
          TagWriteFailed(t, events, existingTagSequenceNrs)
      }

    import context.dispatcher
    withFailure pipeTo self

    context.become(writeInProgress(remainingBuffer, newTagSequenceNrs, Map.empty[String, Long]))
  }

  private def createTagWriteSummary(writes: Seq[(Serialized, TagPidSequenceNr)], events: Vector[Serialized]): Map[PersistenceId, PidProgress] = {
    writes.foldLeft(Map.empty[PersistenceId, PidProgress])((acc, next) => {
      val (event, tagPidSequenceNr) = next
      acc.get(event.persistenceId) match {
        case Some(PidProgress(from, to, _, _)) =>
          if (event.sequenceNr <= to)
            throw new IllegalStateException(s"Expected events to be ordered by seqNr. ${event.persistenceId} " +
              s"Events: ${events.map(e => (e.persistenceId, e.sequenceNr, e.timeUuid))}")
          acc + (event.persistenceId -> PidProgress(from, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
        case None =>
          acc + (event.persistenceId -> PidProgress(event.sequenceNr, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
      }
    })
  }

  private def assignTagPidSequenceNumbers(
    events:                   Vector[Serialized],
    currentTagPidSequenceNrs: Map[String, TagPidSequenceNr]): (Map[String, TagPidSequenceNr], Seq[(Serialized, TagPidSequenceNr)]) = {
    events.foldLeft((currentTagPidSequenceNrs, Vector.empty[(Serialized, TagPidSequenceNr)])) {
      case ((accTagSequenceNrs, accEvents), nextEvent) =>
        val (newSequenceNrs, sequenceNr: TagPidSequenceNr) = calculateTagPidSequenceNr(nextEvent.persistenceId, accTagSequenceNrs)
        (newSequenceNrs, accEvents :+ ((nextEvent, sequenceNr)))
    }
  }
}
