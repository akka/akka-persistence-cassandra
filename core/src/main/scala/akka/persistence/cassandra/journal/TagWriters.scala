/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
import java.lang.{ Integer => JInt, Long => JLong }
import java.net.URLEncoder
import java.util.UUID

import akka.Done
import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded, Props }
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.util.Timeout
import com.datastax.driver.core.{ BatchStatement, PreparedStatement, ResultSet, Statement }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import akka.actor.Timers
import akka.util.ByteString

@InternalApi private[akka] object TagWriters {

  private[akka] case class TagWritersSession(
    tagWritePs:         Future[PreparedStatement],
    tagWriteWithMetaPs: Future[PreparedStatement],
    executeStatement:   Statement => Future[Done],
    selectStatement:    Statement => Future[ResultSet],
    tagProgressPs:      Future[PreparedStatement],
    tagScanningPs:      Future[PreparedStatement]) {

    def writeBatch(tag: Tag, events: Seq[(Serialized, Long)])(implicit ec: ExecutionContext): Future[Done] = {
      val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
      val tagWritePSs = for {
        withMeta <- tagWriteWithMetaPs
        withoutMeta <- tagWritePs
      } yield (withMeta, withoutMeta)

      tagWritePSs.map {
        case (withMeta, withoutMeta) =>
          events.foreach {
            case (event, pidTagSequenceNr) => {
              val ps = if (event.meta.isDefined) withMeta else withoutMeta
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
                event.writerUuid)
              event.meta.foreach { m =>
                bound.setBytes("meta", m.serialized)
                bound.setString("meta_ser_manifest", m.serManifest)
                bound.setInt("meta_ser_id", m.serId)
              }
              batch.add(bound)
            }
          }
          batch
      }.flatMap(executeStatement)
    }

    def writeProgress(tag: Tag, persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(implicit ec: ExecutionContext): Future[Done] = {
      tagProgressPs.map(ps =>
        ps.bind(persistenceId, tag, seqNr: JLong, tagPidSequenceNr: JLong, offset)).flatMap(executeStatement)
    }

  }

  private[akka] object BulkTagWrite {
    def apply(tw: TagWrite): BulkTagWrite = BulkTagWrite(tw :: Nil, Nil)
  }
  private[akka] case class BulkTagWrite(tagWrites: immutable.Seq[TagWrite], withoutTags: immutable.Seq[Serialized])
    extends NoSerializationVerificationNeeded

  private[akka] case class TagWrite(tag: Tag, serialised: immutable.Seq[Serialized])
    extends NoSerializationVerificationNeeded

  def props(settings: TagWriterSettings, tagWriterSession: TagWritersSession): Props =
    Props(new TagWriters(settings, tagWriterSession))

  final case class TagFlush(tag: String)
  case object FlushAllTagWriters
  case object AllFlushed
  final case class PidRecovering(pid: String, tagProgresses: Map[Tag, TagProgress])
  case object PidRecoveringAck
  final case class TagWriteFailed(reason: Throwable)
  private case object WriteTagScanningTick
}

@InternalApi private[akka] class TagWriters(settings: TagWriterSettings, tagWriterSession: TagWritersSession)
  extends Actor with Timers with ActorLogging {

  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log = super.log

  private var tagActors = Map.empty[String, ActorRef]
  // just used for local actor asks
  private implicit val timeout = Timeout(10.seconds)

  private var toBeWrittenScanning: Map[PersistenceId, SequenceNr] = Map.empty
  private var pendingScanning: Map[PersistenceId, SequenceNr] = Map.empty

  timers.startPeriodicTimer(WriteTagScanningTick, WriteTagScanningTick, settings.scanningFlushInterval)

  def receive: Receive = {
    case FlushAllTagWriters =>
      log.debug("Flushing all tag writers")
      // will include a C* write so be patient
      implicit val timeout = Timeout(10.seconds)
      val replyTo = sender()
      val flushes = tagActors.map { case (_, ref) => (ref ? Flush).mapTo[FlushComplete.type] }
      Future.sequence(flushes).map(_ => AllFlushed) pipeTo replyTo
    case TagFlush(tag) =>
      tagActor(tag).tell(Flush, sender())
    case tw: TagWrite =>
      updatePendingScanning(tw.serialised)
      tagActor(tw.tag) forward tw
    case BulkTagWrite(tws, withoutTags) =>
      tws.foreach { tw =>
        updatePendingScanning(tw.serialised)
        tagActor(tw.tag) forward tw
      }
      updatePendingScanning(withoutTags)
    case WriteTagScanningTick =>
      writeTagScanning()

    case PidRecovering(pid, tagProgresses: Map[Tag, TagProgress]) =>
      val replyTo = sender()
      val missingProgress = tagActors.keySet -- tagProgresses.keySet
      pendingScanning -= pid
      log.debug(
        "Recovering pid [{}] with progress [{}]. Tags to reset as not in progress [{}]",
        pid, tagProgresses, missingProgress)
      val tagWriterAcks = Future.sequence(tagProgresses.map {
        case (tag, progress) =>
          log.debug("Sending tag progress: [{}] [{}]", tag, progress)
          (tagActor(tag) ? ResetPersistenceId(tag, progress)).mapTo[ResetPersistenceIdComplete.type]
      })

      // We send an empty progress in case the tag actor has buffered events
      // and has never written any tag progress for this tag/pid
      val blankTagWriterAcks = Future.sequence(missingProgress.map { tag =>
        log.debug("Sending blank progress for tag [{}] pid [{}]", tag, pid)
        (tagActor(tag) ? ResetPersistenceId(tag, TagProgress(pid, 0, 0))).mapTo[ResetPersistenceIdComplete.type]
      })

      val recoveryNotificationComplete = for {
        _ <- tagWriterAcks
        _ <- blankTagWriterAcks
      } yield Done

      // if this fails (all local actor asks) the recovery will timeout
      recoveryNotificationComplete.foreach { _ => replyTo ! PidRecoveringAck }

    case TagWriteFailed(_) =>
      toBeWrittenScanning = Map.empty
      pendingScanning = Map.empty
  }

  private def updatePendingScanning(serialized: immutable.Seq[Serialized]): Unit = {
    serialized.foreach { ser =>
      pendingScanning.get(ser.persistenceId) match {
        case Some(seqNr) =>
          if (ser.sequenceNr > seqNr) // collect highest
            pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
        case None =>
          pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
      }
    }
  }

  private def writeTagScanning(): Unit = {
    val updates = toBeWrittenScanning
    // current pendingScanning will be written on next tick, if no write failures
    toBeWrittenScanning = pendingScanning
    // collect new until next tick
    pendingScanning = Map.empty

    if (updates.nonEmpty && log.isDebugEnabled)
      log.debug("Update tag scanning [{}]", updates.toSeq.mkString(","))

    tagWriterSession.tagScanningPs.foreach { ps =>
      updates.foreach {
        case (pid, seqNr) =>
          tagWriterSession.executeStatement(ps.bind(pid, seqNr: JLong))
            .failed.foreach { t =>
              log.warning("Writing tag scanning failed. Reason {}", t)
              self ! TagWriteFailed(t)
            }
      }
    }
  }

  private def tagActor(tag: String): ActorRef =
    tagActors.get(tag) match {
      case None =>
        val ref = createTagWriter(tag)
        tagActors += (tag -> ref)
        ref
      case Some(ar) => ar
    }

  // protected for testing purposes
  protected def createTagWriter(tag: String): ActorRef = {
    context.actorOf(
      TagWriter.props(settings, tagWriterSession, tag)
        .withDispatcher((context.props.dispatcher)),
      name = URLEncoder.encode(tag, ByteString.UTF_8))
  }

}
