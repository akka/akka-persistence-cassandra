/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import akka.Done
import akka.actor.{ ActorLogging, ActorRef }
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal.Tag
import akka.persistence.cassandra.journal.TagWriter.{ SetTagProgress, TagProgress, TagWrite }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.{ Extractors, RawEvent, TaggedPersistentRepr }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.Timeout

import scala.concurrent._
import scala.concurrent.duration._

trait CassandraRecovery extends CassandraTagRecovery with TaggedPreparedStatements {
  this: CassandraJournal =>

  import config._
  import context.dispatcher

  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long
  )(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    log.debug("Recovering pid {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)

    val recoveryPrep: Future[Map[String, TagProgress]] = for {
      tp <- lookupTagProgress(persistenceId)
      _ <- sendTagProgress(tp)
      startingSequenceNr = calculateStartingSequenceNr(tp)
      _ <- sendPreSnapshotTagWrites(startingSequenceNr, fromSequenceNr, persistenceId, max, tp)
    } yield tp

    Source.fromFutureSource(
      recoveryPrep.map((tp: Map[Tag, TagProgress]) => {
        log.debug("Starting recovery with tag progress: {}", tp)
        queries
          .eventsByPersistenceId(
            persistenceId,
            fromSequenceNr,
            toSequenceNr,
            max,
            replayMaxResultSize,
            None,
            "asyncReplayMessages",
            someReadConsistency,
            someReadRetryPolicy,
            extractor = Extractors.taggedPersistentRepr
          ).map(sendMissingTagWrite(tp, tagWrites))
      })
    ).map(_.pr)
      .runForeach(replayCallback)
      .map(_ => ())
  }

  private def sendPreSnapshotTagWrites(
    minProgressNr:  Long,
    fromSequenceNr: Long,
    pid:            String,
    max:            Long,
    tp:             Map[Tag, TagProgress]
  ): Future[Done] = if (minProgressNr < fromSequenceNr) {
    val scanTo = fromSequenceNr - 1
    log.debug("Scanning events before snapshot to recover tag_views: From: {} to: {}", minProgressNr, scanTo)
    queries.eventsByPersistenceId(
      pid,
      minProgressNr,
      scanTo,
      max,
      replayMaxResultSize,
      None,
      "asyncReplayMessagesPreSnapshot",
      someReadConsistency,
      someReadRetryPolicy,
      Extractors.rawPayloadCurrentSchema(config.bucketSize)
    ).map(sendMissingTagWriteRaw(tp, tagWrites)).runWith(Sink.ignore)
  } else {
    log.debug("Recovery is starting before the latest tag writes tag progress")
    Future.successful(Done)
  }

  private def sendTagProgress(tp: Map[Tag, TagProgress]): Future[Done] = {
    implicit val timeout = Timeout(1.second)
    val progressSets = tp.map {
      case (tag, progress) => (tagWrites ? SetTagProgress(tag, progress)).mapTo[TagWriter.SetTagProgressAck.type]
    }
    Future.sequence(progressSets).map(_ => Done)
  }

  // TODO remove this and always use raw? Add a RawEvent => TaggedPersistentRepr
  private def sendMissingTagWrite(tp: Map[Tag, TagProgress], to: ActorRef)(tpr: TaggedPersistentRepr): TaggedPersistentRepr = {
    tpr.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug("Tag write not in progress. Sending to TagWriter. Tag {} Sequence Nr {}.", tag, tpr.sequenceNr)
          to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset)))
        case Some(progress) =>
          if (tpr.sequenceNr > progress.sequenceNr) {
            log.debug("Sequence nr > than write progress. Sending to TagWriter. Tag {} Sequence Nr {}. ", tag, tpr.sequenceNr)
            to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset)))
          }
      }
    })
    tpr
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val x = asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h))
    }
    x.onComplete(l => log.debug("highest seq nr: {} for pid {}", l, persistenceId))
    x
  }

  private def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {

    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, currentPnr: JLong))
      boundSelectHighestSequenceNr.flatMap(session.selectResultSet)
        .map { rs =>
          Option(rs.one()).map { row =>
            (row.getBool("used"), row.getLong("sequence_nr"))
          }
        }
        .flatMap {
          // never been to this partition
          case None                   => Future.successful(currentSnr)
          // don't currently explicitly set false
          case Some((false, _))       => Future.successful(currentSnr)
          // everything deleted in this partition, move to the next
          case Some((true, 0))        => find(currentPnr + 1, currentSnr)
          case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
        }
    }

    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    val boundSelectDeletedTo = preparedSelectDeletedTo.map(_.bind(persistenceId))
    boundSelectDeletedTo.flatMap(session.selectResultSet)
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }
}
