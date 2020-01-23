/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
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
import akka.cassandra.session._
import akka.serialization.Serialization

trait CassandraRecovery {
  this: CassandraJournal =>

  private[akka] val settings: PluginSettings

  import settings._

  lazy val tagRecovery =
    new CassandraTagRecovery(context.system, session, settings, taggedPreparedStatements)

  private val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(context.system)

  private[akka] def asyncReadHighestSequenceNrInternal(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h), journalSettings.targetPartitionSize)
    }
  }

  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug("[{}] asyncReplayMessages from [{}] to [{}]", persistenceId, fromSequenceNr, toSequenceNr)

    if (eventsByTagSettings.eventsByTagEnabled) {
      val recoveryPrep: Future[Map[String, TagProgress]] = {
        val scanningSeqNrFut = tagRecovery.tagScanningStartingSequenceNr(persistenceId)
        for {
          tp <- tagRecovery.lookupTagProgress(persistenceId)
          _ <- tagRecovery.setTagProgress(persistenceId, tp, tagWrites.get)
          scanningSeqNr <- scanningSeqNrFut
          _ <- sendPreSnapshotTagWrites(scanningSeqNr, fromSequenceNr, persistenceId, max, tp)
        } yield tp
      }

      Source
        .fromFutureSource(recoveryPrep.map((tp: Map[Tag, TagProgress]) => {
          log.debug(
            "[{}] starting recovery with tag progress: [{}]. From [{}] to [{}]",
            persistenceId,
            tp,
            fromSequenceNr,
            toSequenceNr)
          queries
            .eventsByPersistenceId(
              persistenceId,
              fromSequenceNr,
              toSequenceNr,
              max,
              None,
              settings.journalSettings.readProfile,
              "asyncReplayMessages",
              extractor = Extractors.taggedPersistentRepr(eventDeserializer, serialization))
            .mapAsync(1)(tagRecovery.sendMissingTagWrite(tp, tagWrites.get))
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
          None,
          settings.journalSettings.readProfile,
          "asyncReplayMessages",
          extractor = Extractors.persistentRepr(eventDeserializer, serialization))
        .map(p => queries.mapEvent(p.persistentRepr))
        .runForeach(replayCallback)
        .map(_ => ())
    }
  }

  private[akka] def sendPreSnapshotTagWrites(
      minProgressNr: Long,
      fromSequenceNr: Long,
      pid: String,
      max: Long,
      tp: Map[Tag, TagProgress]): Future[Done] = {
    if (minProgressNr < fromSequenceNr) {
      val scanTo = fromSequenceNr - 1
      log.debug(
        "[{}], Scanning events before snapshot to recover tag_views: From: [{}] to: [{}]",
        pid,
        minProgressNr,
        scanTo)
      queries
        .eventsByPersistenceId(
          pid,
          minProgressNr,
          scanTo,
          max,
          None,
          settings.journalSettings.readProfile,
          "asyncReplayMessagesPreSnapshot",
          Extractors.optionalTaggedPersistentRepr(eventDeserializer, serialization))
        .mapAsync(1) { t =>
          t.tagged match {
            case OptionVal.Some(tpr) =>
              tagRecovery.sendMissingTagWrite(tp, tagWrites.get)(tpr)
            case OptionVal.None => FutureDone // no tags, skip
          }
        }
        .runWith(Sink.ignore)
    } else {
      log.debug(
        "[{}] Recovery is starting before the latest tag writes tag progress. Min progress [{}]. From sequence nr of recovery: [{}]",
        pid,
        minProgressNr,
        fromSequenceNr)
      FutureDone
    }
  }
}
