/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.reconciler

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.Extractors
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.serialization.SerializationExtension
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class BuildTagViewForPersistenceId(
    persistenceId: String,
    system: ActorSystem,
    recovery: CassandraTagRecovery,
    settings: PluginSettings) {

  import system.dispatcher

  private implicit val sys: ActorSystem = system
  private val log = Logging(system, classOf[BuildTagViewForPersistenceId])
  private val serialization = SerializationExtension(system)

  private val queries: CassandraReadJournal =
    PersistenceQuery(system.asInstanceOf[ExtendedActorSystem])
      .readJournalFor[CassandraReadJournal]("akka.persistence.cassandra.query")

  private implicit val flushTimeout: Timeout = Timeout(30.seconds)

  /**
   * Rebuilds the tag view for a persistence id by writing ALL events.
   * The current tag progress is used as the baseline for tag_pid_sequence_nr calculation,
   * ensuring idempotency with any concurrent writes from running actors.
   */
  def rebuild(flushEvery: Int = 1000): Future[Done] = {
    val recoveryPrep = for {
      tp <- recovery.lookupTagProgress(persistenceId)
      _ <- recovery.setTagProgress(persistenceId, tp)
    } yield tp

    Source
      .futureSource(recoveryPrep.map((tp: Map[String, TagProgress]) => {
        log.info("[{}] Rebuilding tag view table. Current progress: [{}]", persistenceId, tp)
        queries
          .eventsByPersistenceId(
            persistenceId,
            0,
            Long.MaxValue,
            Long.MaxValue,
            None,
            settings.journalSettings.readProfile,
            "BuildTagViewForPersistenceId",
            extractor = Extractors.rawEvent(settings.eventsByTagSettings.bucketSize, serialization, system))
          .map(recovery.sendTagWriteRaw(actorRunning = false))
          .buffer(flushEvery, OverflowStrategy.backpressure)
          .mapAsync(1)(_ => recovery.flush(flushTimeout))
      }))
      .runWith(Sink.ignore)
  }

  /**
   * Continues tag writes from where they left off by only writing events
   * that are newer than the current progress (seqNr > progress.sequenceNr).
   * Starts reading from the minimum progress sequence number for efficiency.
   */
  def continue(flushEvery: Int = 1000): Future[Done] = {
    val recoveryPrep = for {
      tp <- recovery.lookupTagProgress(persistenceId)
      _ <- recovery.setTagProgress(persistenceId, tp)
    } yield tp

    Source
      .futureSource(recoveryPrep.map((tp: Map[String, TagProgress]) => {
        val fromSeqNr = if (tp.isEmpty) 0L else tp.values.map(_.sequenceNr).min
        log.info("[{}] Continuing tag writes from seqNr [{}]. Current progress: [{}]", persistenceId, fromSeqNr, tp)
        queries
          .eventsByPersistenceId(
            persistenceId,
            fromSeqNr,
            Long.MaxValue,
            Long.MaxValue,
            None,
            settings.journalSettings.readProfile,
            "ContinueTagWritesForPersistenceId",
            extractor = Extractors.rawEvent(settings.eventsByTagSettings.bucketSize, serialization, system))
          .map(recovery.sendMissingTagWriteRaw(tp, actorRunning = false))
          .buffer(flushEvery, OverflowStrategy.backpressure)
          .mapAsync(1)(_ => recovery.flush(flushTimeout))
      }))
      .runWith(Sink.ignore)

  }

}
