/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Integer => JInt, Long => JLong }
import java.util.UUID

import akka.Done
import akka.actor.{ Actor, ActorLogging, NoSerializationVerificationNeeded, Props, Timers }
import akka.annotation.InternalApi
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.pattern.pipe
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.journal.TagWriter.{ TagWriterSession, TagWriterSettings }
import com.datastax.driver.core.{ BatchStatement, PreparedStatement, ResultSet, Statement }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.Try

/*
 * Groups writes into un-logged batches for the same partition.
 *
 * For the read stage to work correctly events must be written in order for a given
 * persistence id for a given tag.
 *
 * Prevents any concurrent writes.
 *
 * FIXME:
 * - Cassandra errors
 *
 * Possible improvements:
 * - Max buffer size
 */
@InternalApi object TagWriter {
  case class TagWriterSettings(
    maxBatchSize:       Int,
    flushInterval:      FiniteDuration,
    pubsubNotification: Boolean
  )

  case class TagProgress(
    persistenceId:    String,
    sequenceNr:       SequenceNr,
    pidTagSequenceNr: SequenceNr
  )

  class TagWriterSession(
    tag:               String,
    tagWritePs:        Future[PreparedStatement],
    executeStatement:  Statement => Future[Done],
    selectStatement:   Statement => Future[ResultSet],
    tagProgressPs:     Future[PreparedStatement],
    tagFirstBucketPs:  Future[PreparedStatement],
    selectTagProgress: Future[PreparedStatement]
  ) {
    def writeBatch(events: Seq[(Serialized, Long)])(implicit ec: ExecutionContext): Future[Done] = {
      val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      tagWritePs.map { ps =>
        events.foreach {
          case (event, pidTagSequenceNr) => {
            val bound = ps.bind(
              tag,
              event.timeBucket.key: JLong,
              event.timeUuid,
              pidTagSequenceNr: JLong,
              event.serialized,
              event.eventAdapterManifest,
              event.persistenceId,
              event.sequenceNr: JLong,
              event.serId: JInt,
              event.serManifest,
              event.writerUuid
            )
            batch.add(bound)
          }
        }
        batch
      }.flatMap(executeStatement)
    }

    def writeTagFirstBucket(persistenceId: String, bucket: TimeBucket)(implicit ec: ExecutionContext): Future[Done] = {
      tagFirstBucketPs.map { ps =>
        {
          ps.bind(
            persistenceId,
            tag,
            bucket.key: JLong
          )
        }
      }.flatMap(executeStatement)
    }

    def selectTagProgress(pid: String)(implicit ec: ExecutionContext): Future[Option[TagProgress]] = {
      selectTagProgress.map { ps =>
        ps.bind(pid, tag)
      }.flatMap(selectStatement).map { rs =>
        Option(rs.one()).map(row => TagProgress(
          pid,
          row.getLong("sequence_nr"),
          row.getLong("tag_pid_sequence_nr")
        ))
      }
    }

    def writeProgress(persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(implicit ec: ExecutionContext): Future[Done] = {
      tagProgressPs.map(ps =>
        ps.bind(persistenceId, tag, seqNr: JLong, tagPidSequenceNr: JLong, offset)).flatMap(executeStatement)
    }
  }

  def props(
    session:  TagWriterSession,
    tag:      String,
    settings: TagWriterSettings
  ): Props =
    Props(classOf[TagWriter], session, tag, settings)

  case class TagWrites(serialised: Vector[Serialized]) extends NoSerializationVerificationNeeded
  case object TagWriteAck

  private[akka] case class TaggedViewWrite(tag: String, event: Serialized)
  private case object Flush
  private case object FlushKey

  type PersistenceId = String
  type SequenceNr = Long
  type TagWriteSummary = Map[PersistenceId, PidProgress]
  case class PidProgress(seqNrFrom: Long, seqNrTo: Long, tagPidSequenceNr: Long, offset: UUID)
  private case class TagWriteDone(summary: TagWriteSummary)
}

@InternalApi class TagWriter(session: TagWriterSession, tag: String, settings: TagWriterSettings)
  extends Actor with Timers with ActorLogging with NoSerializationVerificationNeeded {

  import TagWriter._
  import context._

  log.debug("Running with settings {}", settings)

  private val pubsub: Option[DistributedPubSub] = if (settings.pubsubNotification) {
    Try {
      DistributedPubSub(context.system)
    }.toOption
  } else {
    None
  }

  var sequenceNrs = Map.empty[String, Long]
  var pidTagSequenceNrs = Map.empty[String, Future[Long]]

  override def receive: Receive = idle(Vector.empty[Serialized])

  def validate(payload: Vector[Serialized]): Unit = {
    payload.foreach { s =>
      sequenceNrs.get(s.persistenceId) match {
        case Some(seqNr) =>
          if (seqNr >= s.sequenceNr) {
            throw new IllegalStateException(s"Not received sequence numbers in order: ${s.persistenceId}, currently highest sequenceNr: ${sequenceNrs(s.persistenceId)}. New events: $payload")
          }
          sequenceNrs += (s.persistenceId -> s.sequenceNr)
        case None =>
          sequenceNrs += (s.persistenceId -> s.sequenceNr)
      }
    }

    payload.map(_.persistenceId).foreach(p => log.debug("Updated largest sequence nr for pid {} : {}", p, sequenceNrs(p)))

  }

  private def idle(buffer: Vector[Serialized]): Receive = {
    case Flush =>
      if (buffer.nonEmpty) {
        write(buffer, Vector.empty[Serialized])
      }
    case TagWrites(payload) =>
      validate(payload)
      flushIfRequired(buffer ++ payload)
      sender() ! TagWriteAck
    case TagWriteDone(_) =>
      log.error("Received Done when in idle state. This is a bug. Please report with DEBUG logs")
  }

  private def writeInProgress(buffer: Vector[Serialized]): Receive = {
    case Flush =>
    // Ignore, we will check when the write is in progress
    case TagWrites(payload) =>
      validate(payload)
      // buffer until current query is finished
      become(writeInProgress(buffer ++ payload))
      sender() ! TagWriteAck
    case TagWriteDone(summary) =>
      summary.foreach {
        case (id, p @ PidProgress(_, seqNrTo, tagPidSequenceNr, offset)) =>
          log.debug("Writing tag progress {}", p)
          // These writes do not block future writes. We don't read he tag progress again from C*
          // until a restart has happened. This is best effort and expect recovery (not implemented yet
          // to fix up any missing)
          // TODO Deal with failure and add debug logging for when done
          session.writeProgress(id, seqNrTo, tagPidSequenceNr, offset)
      }
      log.debug(s"Tag write complete. ${summary}")
      flushIfRequired(buffer)
      pubsub.foreach {
        log.debug("Publishing tag update for {}", tag)
        _.mediator ! DistributedPubSubMediator.Publish("akka.persistence.cassandra.journal.tag", tag)
      }
  }

  private def flushIfRequired(buffer: Vector[Serialized]): Unit = {
    if (buffer.isEmpty) {
      context.become(idle(buffer))
    } else if (buffer.head.timeBucket < buffer.last.timeBucket) {
      if (log.isDebugEnabled) {
        log.debug("Switching time buckets: head: {} last: {}", buffer.head.timeBucket, buffer.last.timeBucket)
      }
      val (currentBucket, rest) = buffer.span(_.timeBucket == buffer.head.timeBucket)
      write(currentBucket, rest)
    } else if (buffer.size >= settings.maxBatchSize) {
      log.debug("Batch size reached. Writing msg")
      write(buffer.take(settings.maxBatchSize), buffer.drop(settings.maxBatchSize))
    } else if (settings.flushInterval == Duration.Zero) {
      log.debug("Flushing right away as interval is zero")
      write(buffer, Vector.empty[Serialized])
    } else {
      timers.startSingleTimer(FlushKey, Flush, settings.flushInterval)
      log.debug("Batch size not reached, buffering. Current buffer size: {}", buffer.size)
      context.become(idle(buffer))
    }
  }

  /**
   * The first time we see a pid we go to Cassandra to see if this if this
   * tag/pid has been active before.
   * No other ActorSystem should be reading/updating this row as a pid is owned
   * by one node.
   * If the pid/tag combo has no previous progress we start from 1 otherwise
   * the old pidTagSequenceNr + 1.
   * The future for getting the progress from C* is added to the lookup map
   * so any further requests for that pid have to wait until the initial read
   * is complete.
   */
  private def calculateTagPidSequenceNr(pid: String, bucket: TimeBucket): Future[Long] = {
    // Have we got it already?
    val tagPidSequenceNr: Future[SequenceNr] = pidTagSequenceNrs.get(pid) match {
      case None =>
        log.debug("First time seeing this pId {} since startup", pid)
        session.selectTagProgress(pid).flatMap {
          case None =>
            log.debug("No previous progress for this tag/pid")
            session.writeTagFirstBucket(pid, bucket).map(_ => 1)
          case Some(t @ TagProgress(_, _, pidTagSequenceNr)) =>
            log.debug("Tag progress for new pId: {}", t)
            Future.successful(pidTagSequenceNr + 1)
        }
      case Some(n) =>
        log.debug("Already cached. Previous tagPidSequenceNr: {}", n)
        n.map(_ + 1)
    }
    pidTagSequenceNrs += (pid -> tagPidSequenceNr)
    tagPidSequenceNr
  }

  /**
   * Events should be ordered by sequence nr per pid
   */
  private def write(events: Vector[Serialized], remainingBuffer: Vector[Serialized]): Unit = {
    // Work out all the pidTagSequenceNrs
    val withPidTagSeqNr = events.map(s => {
      calculateTagPidSequenceNr(s.persistenceId, s.timeBucket).map((s, _))
    })

    // Create a summary of this write for logging and to be written to the
    // tag write progress table
    val withSummary = Future.sequence(withPidTagSeqNr)
      .map((wpt: Seq[(Serialized, SequenceNr)]) => {
        val writeSummary = wpt.foldLeft(Map.empty[PersistenceId, PidProgress])((acc, next) => {
          val (event, tagPidSequenceNr) = next
          acc.get(event.persistenceId) match {
            case Some(PidProgress(from, to, _, _)) =>
              if (event.sequenceNr <= to)
                throw new IllegalStateException(s"Expected events to be ordered by seqNr. ${event.persistenceId} Events: ${events.map(e => (e.persistenceId, e.sequenceNr))}")
              acc + (event.persistenceId -> PidProgress(from, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
            case None =>
              acc + (event.persistenceId -> PidProgress(event.sequenceNr, event.sequenceNr, tagPidSequenceNr, event.timeUuid))
          }
        })
        (wpt, writeSummary)
      })

    val done = withSummary.flatMap {
      case (writes, summary) =>
        log.debug("Starting tag write. Summary: {}", summary)
        session.writeBatch(writes).map(_ => TagWriteDone(summary))
    }

    // We never have concurrent writes for the same tag/node, go into writeInProgress state
    context.become(writeInProgress(remainingBuffer))
    done pipeTo self
  }
}
