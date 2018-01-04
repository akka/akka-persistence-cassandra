/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Integer => JInt, Long => JLong }
import java.util.UUID

import akka.Done
import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, Props }
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, Tag }
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

  private[akka] case class BulkTagWrite(tagWrites: Vector[TagWrite])

  def props(tagWriterCreator: (ActorRefFactory, Tag) => ActorRef): Props =
    Props(new TagWriters(tagWriterCreator))

  case class TagFlush(tag: String)
  case object FlushAllTagWriters
  case object AllFlushed
}

@InternalApi private[akka] class TagWriters(tagWriterCreator: (ActorRefFactory, Tag) => ActorRef)
  extends Actor with ActorLogging {

  import context._

  private var tagActors = Map.empty[String, ActorRef]

  def receive: Receive = {
    case FlushAllTagWriters =>
      log.debug("Flushing all tag writers")
      // will include a C* write so be patient
      implicit val timeout = Timeout(10.seconds)
      val replyTo = sender()
      val flushes = tagActors.map { case (_, ref) => (ref ? Flush).mapTo[Flushed.type] }
      Future.sequence(flushes).map(_ => AllFlushed) pipeTo replyTo
    case twf: TagFlush =>
      tagActor(twf.tag).tell(Flush, sender())
    case tw: TagWrite =>
      tagActor(tw.tag) forward tw
    case BulkTagWrite(tws) =>
      tws.foreach(tw => {
        tagActor(tw.tag) forward tw
      })
    case p: SetTagProgress =>
      tagActor(p.tag) forward p
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
