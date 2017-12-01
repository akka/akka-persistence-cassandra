/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraJournal.{ SequenceNr, Tag }
import akka.persistence.cassandra.journal.TagWriter.{ SetTagProgress, TagProgress, TagWrite }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.RawEvent
import akka.util.Timeout

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

trait CassandraTagRecovery {
  self: TaggedPreparedStatements =>
  protected val log: LoggingAdapter

  // No other writes for this pid should be taking place during recovery
  // The result set size will be the number of distinct tags that this pid has used, expecting
  // that to be small (<10) so call to all should be safe
  private[akka] def lookupTagProgress(persistenceId: String)(implicit ec: ExecutionContext): Future[Map[Tag, TagProgress]] =
    preparedSelectTagProgressForPersistenceId.map(_.bind(persistenceId))
      .flatMap(session.selectResultSet)
      .map(rs => rs.all().asScala.foldLeft(Map.empty[String, TagProgress]) { (acc, row) =>
        acc + (row.getString("tag") -> TagProgress(persistenceId, row.getLong("sequence_nr"), row.getLong("tag_pid_sequence_nr")))
      })

  // Before starting the actual recovery first go from the oldest tag progress -> fromSequenceNr
  // and fix any tags. This recovers any tag writes that happened before the latest snapshot
  private[akka] def calculateStartingSequenceNr(tps: Map[Tag, TagProgress]): SequenceNr =
    if (tps.isEmpty) 1L
    else
      tps.foldLeft(Long.MaxValue) {
        case (currentMin, (_, TagProgress(_, sequenceNr, _))) => math.min(currentMin, sequenceNr)
      }

  private[akka] def sendMissingTagWriteRaw(tp: Map[Tag, TagProgress], to: ActorRef)(rawEvent: RawEvent): RawEvent = {
    // FIXME logging before releasing
    log.debug("Processing tag write for event {} tags {}", rawEvent.sequenceNr, rawEvent.serialized.tags)
    rawEvent.serialized.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug("Tag write not in progress. Sending to TagWriter. Tag {} Sequence Nr {}.", tag, rawEvent.sequenceNr)
          to ! TagWrite(tag, Vector(rawEvent.serialized))
        case Some(progress) =>
          if (rawEvent.sequenceNr > progress.sequenceNr) {
            log.debug("Sequence nr > than write progress. Sending to TagWriter. Tag {} Sequence Nr {}. ", tag, rawEvent.sequenceNr)
            to ! TagWrite(tag, Vector(rawEvent.serialized))
          }
      }
    })
    rawEvent
  }

  private[akka] def sendTagProgress(tp: Map[Tag, TagProgress], ref: ActorRef): Future[Done] = {
    implicit val timeout = Timeout(1.second)
    val progressSets = tp.map {
      case (tag, progress) => (ref ? SetTagProgress(tag, progress)).mapTo[TagWriter.SetTagProgressAck.type]
    }
    Future.sequence(progressSets).map(_ => Done)
  }

}
