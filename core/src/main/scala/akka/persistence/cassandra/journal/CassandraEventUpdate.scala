/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.event.LoggingAdapter
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, TagPidSequenceNr }
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row, Statement }
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import java.lang.{ Long => JLong }

import akka.annotation.InternalApi
import akka.persistence.cassandra.CachedPreparedStatement
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

/** INTERNAL API */
@InternalApi private[akka] trait CassandraEventUpdate {

  private[akka] val session: CassandraSession
  private[akka] def settings: PluginSettings
  private[akka] implicit val ec: ExecutionContext
  private[akka] val log: LoggingAdapter

  private def journalSettings = settings.journalSettings
  private lazy val journalStatements = new CassandraJournalStatements(settings)
  val psUpdateMessage: CachedPreparedStatement =
    new CachedPreparedStatement(() => session.prepare(journalStatements.updateMessagePayloadAndTags))
  val psSelectTagPidSequenceNr: CachedPreparedStatement =
    new CachedPreparedStatement(() => session.prepare(journalStatements.selectTagPidSequenceNr))
  val psUpdateTagView: CachedPreparedStatement =
    new CachedPreparedStatement(() => session.prepare(journalStatements.updateMessagePayloadInTagView))
  val psSelectMessages: CachedPreparedStatement =
    new CachedPreparedStatement(() => session.prepare(journalStatements.selectMessages))

  /**
   * Update the given event in the messages table and the tag_views table.
   *
   * Does not support changing tags in anyway. The tags field is ignored.
   */
  def updateEvent(event: Serialized): Future[Done] =
    for {
      (partitionNr, existingTags) <- findEvent(event)
      psUM <- psUpdateMessage.get()
      e = event.copy(tags = existingTags) // do not allow updating of tags
      _ <- session.executeWrite(prepareUpdate(psUM, e, partitionNr))
      _ <- Future.traverse(existingTags) { tag =>
        updateEventInTagViews(event, tag)
      }
    } yield Done

  private def findEvent(s: Serialized): Future[(Long, Set[String])] = {
    val firstPartition = partitionNr(s.sequenceNr, journalSettings.targetPartitionSize)
    for {
      ps <- psSelectMessages.get()
      row <- findEvent(ps, s.persistenceId, s.sequenceNr, firstPartition)
    } yield (row.getLong("partition_nr"), row.getSet[String]("tags", classOf[String]).asScala.toSet)
  }

  /**
   * Events are nearly always in a deterministic partition. However they can be in the
   * N + 1 partition if a large atomic write was done.
   */
  private def findEvent(ps: PreparedStatement, pid: String, sequenceNr: Long, partitionNr: Long): Future[Row] =
    session
      .selectOne(ps.bind(pid, partitionNr: JLong, sequenceNr: JLong, sequenceNr: JLong))
      .flatMap {
        case Some(row) => Future.successful(Some(row))
        case None =>
          session.selectOne(pid, partitionNr + 1: JLong, sequenceNr: JLong, sequenceNr: JLong)
      }
      .map {
        case Some(row) => row
        case None =>
          throw new RuntimeException(
            s"Unable to find event: Pid: [$pid] SequenceNr: [$sequenceNr] partitionNr: [$partitionNr]")
      }

  private def updateEventInTagViews(event: Serialized, tag: String): Future[Done] =
    psSelectTagPidSequenceNr
      .get()
      .flatMap { ps =>
        val bound = ps
          .bind()
          .setString("tag_name", tag)
          .setLong("timebucket", event.timeBucket.key)
          .setUuid("timestamp", event.timeUuid)
          .setString("persistence_id", event.persistenceId)
        session.selectOne(bound)
      }
      .map {
        case Some(r) => r.getLong("tag_pid_sequence_nr")
        case None =>
          throw new RuntimeException(
            s"no tag pid sequence nr. Pid ${event.persistenceId}. Tag: $tag. SequenceNr: ${event.sequenceNr}")
      }
      .flatMap { tagPidSequenceNr =>
        updateEventInTagViews(event, tag, tagPidSequenceNr)
      }

  private def updateEventInTagViews(event: Serialized, tag: String, tagPidSequenceNr: TagPidSequenceNr): Future[Done] =
    psUpdateTagView.get().flatMap { ps =>
      // primary key
      val bound = ps
        .bind()
        .setString("tag_name", tag)
        .setLong("timebucket", event.timeBucket.key)
        .setUuid("timestamp", event.timeUuid)
        .setString("persistence_id", event.persistenceId)
        .setLong("tag_pid_sequence_nr", tagPidSequenceNr)
        .setByteBuffer("event", event.serialized)
        .setString("ser_manifest", event.serManifest)
        .setInt("ser_id", event.serId)
        .setString("event_manifest", event.eventAdapterManifest)

      session.executeWrite(bound)
    }

  private def prepareUpdate(ps: PreparedStatement, s: Serialized, partitionNr: Long): Statement[_] = {
    // primary key
    ps.bind()
      .setString("persistence_id", s.persistenceId)
      .setLong("partition_nr", partitionNr)
      .setLong("sequence_nr", s.sequenceNr)
      .setUuid("timestamp", s.timeUuid)
      .setInt("ser_id", s.serId)
      .setString("ser_manifest", s.serManifest)
      .setString("event_manifest", s.eventAdapterManifest)
      .setByteBuffer("event", s.serialized)
      .setSet("tags", s.tags.asJava, classOf[String])
  }
}
