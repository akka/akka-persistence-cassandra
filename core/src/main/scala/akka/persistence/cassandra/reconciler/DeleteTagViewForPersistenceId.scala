/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.ActorSystem
import akka.persistence.cassandra.PluginSettings
import akka.Done
import akka.event.Logging
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.cassandra.journal.TimeBucket
import akka.stream.Materializer
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

/**
 * Deletes tagged events for a persistence id.
 *
 * Walks the events by tags table for the given tag, filters by persistence id, and
 * issues deletes one at a time.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class DeleteTagViewForPersistenceId(
    persistenceIds: Set[String],
    tag: String,
    system: ActorSystem,
    session: ReconciliationSession,
    settings: PluginSettings,
    queries: CassandraReadJournal) {
  private val log = Logging(system, s"DeleteTagView($tag)")
  private implicit val sys: ActorSystem = system
  import system.dispatcher

  def execute(): Future[Done] = {
    queries
      .currentEventsByTagInternal(tag, NoOffset)
      .filter(persistenceIds contains _.persistentRepr.persistenceId)
      // Make the parallelism configurable?
      .mapAsync(1) { uuidPr =>
        val bucket = TimeBucket(uuidPr.offset, settings.eventsByTagSettings.bucketSize)
        val timestamp = uuidPr.offset
        val persistenceId = uuidPr.persistentRepr.persistenceId
        val tagPidSequenceNr = uuidPr.tagPidSequenceNr
        log.debug("Issuing delete {} {} {} {}", persistenceId, bucket, timestamp, tagPidSequenceNr)
        session.deleteFromTagView(tag, bucket, timestamp, persistenceId, tagPidSequenceNr)
      }
      .runWith(Sink.ignore)
      .flatMap(_ =>
        Future.traverse(persistenceIds) { pid =>
          val progress = session.deleteTagProgress(tag, pid)
          val scanning = session.deleteTagScannning(pid)
          for {
            _ <- progress
            _ <- scanning
          } yield Done
        })
      .map(_ => Done)
  }

}
