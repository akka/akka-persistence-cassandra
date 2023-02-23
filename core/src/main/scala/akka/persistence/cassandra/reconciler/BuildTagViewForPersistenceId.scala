/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.ActorSystem
import akka.persistence.cassandra.PluginSettings
import akka.Done
import akka.persistence.cassandra.journal.TagWriter._

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.actor.ExtendedActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.Extractors
import akka.util.Timeout
import akka.stream.{ Materializer, OverflowStrategy }
import akka.stream.scaladsl.Sink
import akka.annotation.InternalApi
import akka.serialization.SerializationExtension

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class BuildTagViewForPersisetceId(
    persistenceId: String,
    system: ActorSystem,
    recovery: CassandraTagRecovery,
    settings: PluginSettings) {

  import system.dispatcher

  private implicit val sys: ActorSystem = system
  private val log = Logging(system, classOf[BuildTagViewForPersisetceId])
  private val serialization = SerializationExtension(system)

  private val queries: CassandraReadJournal =
    PersistenceQuery(system.asInstanceOf[ExtendedActorSystem])
      .readJournalFor[CassandraReadJournal]("akka.persistence.cassandra.query")

  private implicit val flushTimeout: Timeout = Timeout(30.seconds)

  def reconcile(flushEvery: Int = 1000): Future[Done] = {

    val recoveryPrep = for {
      tp <- recovery.lookupTagProgress(persistenceId)
      _ <- recovery.setTagProgress(persistenceId, tp)
    } yield tp

    Source
      .futureSource(recoveryPrep.map((tp: Map[String, TagProgress]) => {
        log.debug("[{}] Rebuilding tag view table from: [{}]", persistenceId, tp)
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
          .map(recovery.sendMissingTagWriteRaw(tp, actorRunning = false))
          .buffer(flushEvery, OverflowStrategy.backpressure)
          .mapAsync(1)(_ => recovery.flush(flushTimeout))
      }))
      .runWith(Sink.ignore)

  }

}
