/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.TagWriter.{ SetTagProgress, TagWrite, TagWriterSession, TagWriterSettings }
import akka.persistence.cassandra.journal.TagWriters.{ BulkTagWrite, TagWritersSession }
import com.datastax.driver.core.{ PreparedStatement, ResultSet, Statement }

import scala.concurrent.Future

@InternalApi private[akka] object TagWriters {

  case class TagWritersSession(
    tagWritePs:         Future[PreparedStatement],
    tagWriteWithMetaPs: Future[PreparedStatement],
    executeStatement:   Statement => Future[Done],
    selectStatement:    Statement => Future[ResultSet],
    tagProgressPs:      Future[PreparedStatement]
  )

  private[akka] case class BulkTagWrite(tagWrites: Vector[TagWrite])

  def props(session: TagWritersSession, tagWriterSettings: TagWriterSettings): Props =
    Props(new TagWriters(session, tagWriterSettings))
}

@InternalApi private[akka] class TagWriters(session: TagWritersSession, tagWriterSettings: TagWriterSettings) extends Actor
  with ActorLogging {

  private var tagActors = Map.empty[String, ActorRef]

  def receive: Receive = {
    case tw: TagWrite =>
      tagActor(tw.tag) forward tw
    case BulkTagWrite(tws) =>
      tws.foreach(tw => {
        tagActor(tw.tag) forward tw
      })
    case p: SetTagProgress =>
      tagActor(p.tag) forward p
    case akka.actor.Status.Failure(_) =>
      log.debug("Failed tow write to Cassandra so will not do TagWrites")
  }

  protected def tagActor(tag: String): ActorRef =
    tagActors.get(tag) match {
      case None =>
        val tagSession = new TagWriterSession(
          tag,
          session.tagWritePs,
          session.tagWriteWithMetaPs,
          session.executeStatement,
          session.selectStatement,
          session.tagProgressPs
        )
        val ref = context.actorOf(
          TagWriter.props(tagSession, tag, tagWriterSettings), s"tag-writer-$tag"
        )
        tagActors += (tag -> ref)
        ref
      case Some(ar) => ar
    }
}
