/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.actor.ActorRef
import akka.persistence.PersistentRepr
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.CassandraJournal.Tag
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.persistence.cassandra.journal.TagWriters.TagWrite
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.{ Extractors, TaggedPersistentRepr }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.OptionVal
import scala.concurrent._

trait CassandraRecovery extends CassandraTagRecovery
  with TaggedPreparedStatements {
  this: CassandraJournal =>

  private[akka] val config: CassandraJournalConfig

  import config._

  private[akka] val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(context.system)

  private[akka] def asyncReadHighestSequenceNrInternal(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h), targetPartitionSize)
    }
  }

  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug("Recovering pid {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)

    if (config.eventsByTagEnabled) {
      val recoveryPrep: Future[Map[String, TagProgress]] = {
        val scanningSeqNrFut = tagScanningStartingSequenceNr(persistenceId)
        for {
          tp <- lookupTagProgress(persistenceId)
          _ <- sendTagProgress(persistenceId, tp, tagWrites.get)
          scanningSeqNr <- scanningSeqNrFut
          _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, max, tp)
        } yield tp
      }

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
              extractor = Extractors.taggedPersistentRepr(eventDeserializer, serialization)).mapAsync(1)(sendMissingTagWrite(tp, tagWrites.get))
        }))
        .map(te => queries.mapEvent(te.pr))
        .runForeach(replayCallback)
        .map(_ => ())

    } else {
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
          extractor = Extractors.persistentRepr(eventDeserializer, serialization))
        .map(p => queries.mapEvent(p.persistentRepr))
        .runForeach(replayCallback)
        .map(_ => ())
    }
  }

  private[akka] def sendPreSnapshotTagWrites(
    minProgressNr:  Long,
    fromSequenceNr: Long,
    pid:            String,
    max:            Long,
    tp:             Map[Tag, TagProgress]): Future[Done] = if (minProgressNr < fromSequenceNr) {
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
      Extractors.optionalTaggedPersistentRepr(eventDeserializer, serialization))
      .mapAsync(1) { t =>
        t.tagged match {
          case OptionVal.Some(tpr) => sendMissingTagWrite(tp, tagWrites.get)(tpr)
          case OptionVal.None      => FutureDone // no tags, skip
        }
      }
      .runWith(Sink.ignore)
  } else {
    log.debug("Recovery is starting before the latest tag writes tag progress. Min progress for pid {}. " +
      "From sequence nr of recovery: {}", minProgressNr, fromSequenceNr)
    FutureDone
  }

  // TODO migrate this to using raw, maybe after offering a way to migrate old events in message?
  private def sendMissingTagWrite(tp: Map[Tag, TagProgress], to: ActorRef)(tpr: TaggedPersistentRepr): Future[TaggedPersistentRepr] = {
    if (tpr.tags.isEmpty) Future.successful(tpr)
    else {
      val completed: List[Future[Done]] =
        tpr.tags.toList.map(tag => tag -> serializeEvent(tpr.pr, tpr.tags, tpr.offset, bucketSize, serialization, context.system))
          .map {
            case (tag, serializedFut) => serializedFut.map { serialized =>
              tp.get(tag) match {
                case None =>
                  log.debug("Tag write not in progress. Sending to TagWriter. Tag {} Sequence Nr {}.", tag, tpr.sequenceNr)
                  to ! TagWrite(tag, serialized :: Nil)
                  Done
                case Some(progress) =>
                  if (tpr.sequenceNr > progress.sequenceNr) {
                    log.debug("Sequence nr > than write progress. Sending to TagWriter. Tag {} Sequence Nr {}. ", tag, tpr.sequenceNr)
                    to ! TagWrite(tag, serialized :: Nil)
                  }
                  Done
              }
            }
          }

      Future.sequence(completed).map(_ => tpr)
    }
  }
}
