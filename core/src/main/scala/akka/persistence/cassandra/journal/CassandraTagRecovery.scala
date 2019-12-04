/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraJournal.{ SequenceNr, Tag }
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.persistence.cassandra.journal.TagWriters.{
  PersistentActorStarting,
  PersistentActorStartingAck,
  SetTagProgress,
  TagProcessAck,
  TagWrite
}
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.RawEvent
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

trait CassandraTagRecovery {
  self: TaggedPreparedStatements =>
  private[akka] val log: LoggingAdapter

  // used for local asks
  private implicit val timeout = Timeout(10.second)

  // No other writes for this pid should be taking place during recovery
  // The result set size will be the number of distinct tags that this pid has used, expecting
  // that to be small (<10) so call to all should be safe
  private[akka] def lookupTagProgress(
      persistenceId: String)(implicit ec: ExecutionContext, mat: Materializer): Future[Map[Tag, TagProgress]] =
    preparedSelectTagProgressForPersistenceId
      .map(_.bind(persistenceId).setExecutionProfileName(config.readProfile))
      .flatMap(stmt => {
        session.select(stmt).runWith(Sink.seq)
      })
      .map(rs =>
        rs.foldLeft(Map.empty[String, TagProgress]) { (acc, row) =>
          acc + (row.getString("tag") -> TagProgress(
            persistenceId,
            row.getLong("sequence_nr"),
            row.getLong("tag_pid_sequence_nr")))
        })

  // Before starting the actual recovery first go from the oldest tag progress -> fromSequenceNr
  // or min tag scanning sequence number, and fix any tags. This recovers any tag writes that
  // happened before the latest snapshot
  private[akka] def tagScanningStartingSequenceNr(persistenceId: String): Future[SequenceNr] =
    preparedSelectTagScanningForPersistenceId
      .map(_.bind(persistenceId).setExecutionProfileName(config.readProfile))
      .flatMap(session.selectOne)
      .map {
        case Some(row) => row.getLong("sequence_nr")
        case None      => 1L
      }

  private[akka] def sendMissingTagWriteRaw(tp: Map[Tag, TagProgress], to: ActorRef, actorRunning: Boolean = true)(
      rawEvent: RawEvent): RawEvent = {
    // FIXME logging once stable
    log.debug("Processing tag write for event {} tags {}", rawEvent.sequenceNr, rawEvent.serialized.tags)
    rawEvent.serialized.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug(
            "[{}] Tag write not in progress. Sending to TagWriter. Tag [{}] seqNr [{}]",
            rawEvent.serialized.persistenceId,
            tag,
            rawEvent.sequenceNr)
          to ! TagWrite(tag, rawEvent.serialized :: Nil, actorRunning)
        case Some(progress) =>
          if (rawEvent.sequenceNr > progress.sequenceNr) {
            log.debug(
              "[{}] seqNr > than write progress. Sending to TagWriter. Tag {} seqNr {}. ",
              rawEvent.serialized.persistenceId,
              tag,
              rawEvent.sequenceNr)
            to ! TagWrite(tag, rawEvent.serialized :: Nil, actorRunning)
          }
      }
    })
    rawEvent
  }

  /**
   * Before starting to process tagged messages then a [SetTagProgress] is sent to the
   * [TagWriters] to initialise the sequence numbers for each tag.
   */
  private[akka] def setTagProgress(
      pid: String,
      tagProgress: Map[Tag, TagProgress],
      tagWriters: ActorRef): Future[Done] = {
    log.debug("[{}] Recovery sending tag progress: [{}]", pid, tagProgress)
    (tagWriters ? SetTagProgress(pid, tagProgress)).mapTo[TagProcessAck.type].map(_ => Done)
  }

  private[akka] def sendPersistentActorStarting(
      pid: String,
      persistentActor: ActorRef,
      tagWriters: ActorRef): Future[Done] = {
    log.debug("[{}] Persistent actor starting [{}]", pid, persistentActor)
    (tagWriters ? PersistentActorStarting(pid, persistentActor)).mapTo[PersistentActorStartingAck.type].map(_ => Done)
  }

}
