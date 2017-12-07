/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.CassandraJournal.Tag
import akka.persistence.cassandra.journal.TagWriter.{ SetTagProgress, TagProgress, TagWrite }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.{ Extractors, TaggedPersistentRepr }
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent._

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
      _ <- sendTagProgress(tp, tagWrites)
      startingSequenceNr = calculateStartingSequenceNr(tp)
      _ <- sendPreSnapshotTagWrites(startingSequenceNr, fromSequenceNr, persistenceId, max, tp)
    } yield tp

    Source.fromFutureSource(
      recoveryPrep.map((tp: Map[Tag, TagProgress]) => {
        log.debug("Starting recovery with tag progress: {}. From {} to {}", tp, fromSequenceNr, toSequenceNr)
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
    ).map(te => queries.mapEvent(te.pr))
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
      Extractors.taggedPersistentRepr
    ).map(sendMissingTagWrite(tp, tagWrites)).runWith(Sink.ignore)
  } else {
    log.debug("Recovery is starting before the latest tag writes tag progress. Min progress for pid {}. " +
      "From sequence nr of recovery: {}", minProgressNr, fromSequenceNr)
    Future.successful(Done)
  }

  // TODO migrate this to using raw, maybe after offering a way to migrate old events in message?
  private def sendMissingTagWrite(tp: Map[Tag, TagProgress], to: ActorRef)(tpr: TaggedPersistentRepr): TaggedPersistentRepr = {
    tpr.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug("Tag write not in progress. Sending to TagWriter. Tag {} Sequence Nr {}.", tag, tpr.sequenceNr)
          to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset, bucketSize, serialization, transportInformation)))
        case Some(progress) =>
          if (tpr.sequenceNr > progress.sequenceNr) {
            log.debug("Sequence nr > than write progress. Sending to TagWriter. Tag {} Sequence Nr {}. ", tag, tpr.sequenceNr)
            to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset, bucketSize, serialization, transportInformation)))
          }
      }
    })
    tpr
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h))
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
