/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.persistence.cassandra.Extractors.RawEvent
import akka.stream.scaladsl.Sink
import akka.util.Timeout

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.Extractors.TaggedPersistentRepr
import akka.serialization.SerializationExtension
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TagWriters.FlushAllTagWriters
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraTagRecovery(
    system: ActorSystem,
    session: CassandraSession,
    settings: PluginSettings,
    statements: TaggedPreparedStatements,
    tagWriters: ActorRef) {

  private implicit val sys: ActorSystem = system
  private val log: LoggingAdapter = Logging(system, classOf[CassandraTagRecovery])
  private val serialization = SerializationExtension(system)

  // used for local asks
  private implicit val timeout: Timeout = Timeout(10.second)

  import statements._

  // No other writes for this pid should be taking place during recovery
  // The result set size will be the number of distinct tags that this pid has used, expecting
  // that to be small (<10) so call to all should be safe
  def lookupTagProgress(persistenceId: String)(implicit ec: ExecutionContext): Future[Map[Tag, TagProgress]] =
    selectTagProgressForPersistenceId
      .get()
      .map(_.bind(persistenceId).setExecutionProfileName(settings.journalSettings.readProfile))
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
  def tagScanningStartingSequenceNr(persistenceId: String): Future[SequenceNr] =
    selectTagScanningForPersistenceId
      .get()
      .map(_.bind(persistenceId).setExecutionProfileName(settings.journalSettings.readProfile))
      .flatMap(session.selectOne)
      .map {
        case Some(row) => row.getLong("sequence_nr")
        case None      => 1L
      }

  def sendMissingTagWrite(tagProgress: Map[Tag, TagProgress])(tpr: TaggedPersistentRepr): Future[TaggedPersistentRepr] =
    if (tpr.tags.isEmpty) Future.successful(tpr)
    else {
      val completed: List[Future[Done]] =
        tpr.tags.toList
          .map(
            tag =>
              tag -> serializeEvent(
                tpr.pr,
                tpr.tags,
                tpr.offset,
                settings.eventsByTagSettings.bucketSize,
                serialization,
                system))
          .map {
            case (tag, serializedFut) =>
              serializedFut.map { serialized =>
                tagProgress.get(tag) match {
                  case None =>
                    log.debug(
                      "[{}] Tag write not in progress. Sending to TagWriter. Tag [{}] Sequence Nr [{}]",
                      tpr.pr.persistenceId,
                      tag,
                      tpr.sequenceNr)
                    tagWriters ! TagWrite(tag, serialized :: Nil)
                    Done
                  case Some(progress) =>
                    if (tpr.sequenceNr > progress.sequenceNr) {
                      log.debug(
                        "[{}] Sequence nr > than write progress. Sending to TagWriter. Tag [{}] Sequence Nr [{}]",
                        tpr.pr.persistenceId,
                        tag,
                        tpr.sequenceNr)
                      tagWriters ! TagWrite(tag, serialized :: Nil)
                    }
                    Done
                }
              }
          }

      Future.sequence(completed).map(_ => tpr)
    }

  def sendMissingTagWriteRaw(tp: Map[Tag, TagProgress], actorRunning: Boolean = true)(rawEvent: RawEvent): RawEvent = {
    rawEvent.serialized.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug(
            "[{}] Tag write not in progress. Sending to TagWriter. Tag [{}] seqNr [{}]",
            rawEvent.serialized.persistenceId,
            tag,
            rawEvent.sequenceNr)
          tagWriters ! TagWrite(tag, rawEvent.serialized :: Nil, actorRunning)
        case Some(progress) =>
          if (rawEvent.sequenceNr > progress.sequenceNr) {
            log.debug(
              "[{}] seqNr > than write progress. Sending to TagWriter. Tag {} seqNr {}. ",
              rawEvent.serialized.persistenceId,
              tag,
              rawEvent.sequenceNr)
            tagWriters ! TagWrite(tag, rawEvent.serialized :: Nil, actorRunning)
          }
      }
    })
    rawEvent
  }

  def flush(timeout: Timeout): Future[Done] = {
    (tagWriters ? FlushAllTagWriters(timeout))(Timeout(timeout.duration * 2)).map(_ => Done)
  }

  /**
   * Before starting to process tagged messages then a [SetTagProgress] is sent to the
   * [TagWriters] to initialise the sequence numbers for each tag.
   */
  def setTagProgress(pid: String, tagProgress: Map[Tag, TagProgress]): Future[Done] = {
    log.debug("[{}] Recovery sending tag progress: [{}]", pid, tagProgress)
    (tagWriters ? SetTagProgress(pid, tagProgress)).mapTo[TagProcessAck.type].map(_ => Done)
  }

  def sendPersistentActorStarting(pid: String, persistentActor: ActorRef): Future[Done] = {
    log.debug("[{}] Persistent actor starting [{}]", pid, persistentActor)
    (tagWriters ? PersistentActorStarting(pid, persistentActor)).mapTo[PersistentActorStartingAck.type].map(_ => Done)
  }

}
