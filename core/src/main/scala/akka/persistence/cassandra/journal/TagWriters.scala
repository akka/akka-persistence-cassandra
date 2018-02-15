/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Integer => JInt, Long => JLong }
import java.util.UUID

import akka.Done
import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, NoSerializationVerificationNeeded, Props }
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.util.Timeout
import com.datastax.driver.core.{ BatchStatement, PreparedStatement, ResultSet, Statement }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

@InternalApi private[akka] object TagWriters {

  private[akka] case class TagWritersSession(
    tagWritePs:         Future[PreparedStatement],
    tagWriteWithMetaPs: Future[PreparedStatement],
    executeStatement:   Statement => Future[Done],
    selectStatement:    Statement => Future[ResultSet],
    tagProgressPs:      Future[PreparedStatement]
  ) {

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
                event.writerUuid
              )
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

  private[akka] case class BulkTagWrite(tagWrites: Vector[TagWrite]) extends NoSerializationVerificationNeeded

  def props(tagWriterCreator: (ActorRefFactory, Tag) => ActorRef): Props =
    Props(new TagWriters(tagWriterCreator))

  case class TagFlush(tag: String)
  case object FlushAllTagWriters
  case object AllFlushed
  case class PidRecovering(pid: String, tagProgresses: Map[Tag, TagProgress])
  case object PidRecoveringAck
}

@InternalApi private[akka] class TagWriters(tagWriterCreator: (ActorRefFactory, Tag) => ActorRef)
  extends Actor with ActorLogging {

  import context._

  private var tagActors = Map.empty[String, ActorRef]
  // just used for local actor asks
  private implicit val timeout = Timeout(10.seconds)

  def receive: Receive = {
    case FlushAllTagWriters =>
      log.debug("Flushing all tag writers")
      // will include a C* write so be patient
      implicit val timeout = Timeout(10.seconds)
      val replyTo = sender()
      val flushes = tagActors.map { case (_, ref) => (ref ? Flush).mapTo[FlushComplete.type] }
      Future.sequence(flushes).map(_ => AllFlushed) pipeTo replyTo
    case twf: TagFlush =>
      tagActor(twf.tag).tell(Flush, sender())
    case tw: TagWrite =>
      tagActor(tw.tag) forward tw
    case BulkTagWrite(tws) =>
      tws.foreach(tw => {
        tagActor(tw.tag) forward tw
      })
    case PidRecovering(pid, tagProgresses: Map[Tag, TagProgress]) =>
      val replyTo = sender()
      val missingProgress = tagActors.keySet -- tagProgresses.keySet
      log.debug("Recovering pid with progress [{}]. Tags to reset as not in progress [{}]", tagProgresses, missingProgress)
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

    case akka.actor.Status.Failure(_) =>
      log.debug("Failed to write to Cassandra so will not do TagWrites")
  }

  protected def tagActor(tag: String): ActorRef =
    tagActors.get(tag) match {
      case None =>
        val ref = tagWriterCreator(context, tag)
        tagActors += (tag -> ref)
        ref
      case Some(ar) => ar
    }
}
