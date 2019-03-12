/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, TagPidSequenceNr }
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.{ PreparedStatement, Row, Statement }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

import java.lang.{ Long => JLong }

private[akka] trait CassandraEventUpdate extends CassandraStatements {

  private[akka] val session: CassandraSession
  private[akka] def config: CassandraJournalConfig
  private[akka] implicit val ec: ExecutionContext
  private[akka] val log: LoggingAdapter

  def psUpdateMessage: Future[PreparedStatement] =
    session.prepare(updateMessagePayloadAndTags).map(_.setIdempotent(true))
  def psSelectTagPidSequenceNr: Future[PreparedStatement] =
    session.prepare(selectTagPidSequenceNr).map(_.setIdempotent(true))
  def psUpdateTagView: Future[PreparedStatement] =
    session.prepare(updateMessagePayloadInTagView).map(_.setIdempotent(true))
  def psSelectMessages: Future[PreparedStatement] =
    session.prepare(selectMessages).map(_.setIdempotent(true))

  /**
   * Update the given event in the messages table and the tag_views table.
   *
   * Does not support changing tags in anyway. The tags field is ignored.
   */
  def updateEvent(event: Serialized): Future[Done] =
    for {
      (partitionNr, existingTags) <- findEvent(event)
      psUM <- psUpdateMessage
      e = event.copy(tags = existingTags) // do not allow updating of tags
      _ <- session.executeWrite(prepareUpdate(psUM, e, partitionNr))
      _ <- Future.traverse(existingTags) { tag =>
        updateEventInTagViews(event, tag)
      }
    } yield Done

  private def findEvent(s: Serialized): Future[(Long, Set[String])] = {
    val firstPartition = partitionNr(s.sequenceNr, config.targetPartitionSize)
    for {
      ps <- psSelectMessages
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
      .flatMap { ps =>
        val bind = ps.bind()
        bind.setString("tag_name", tag)
        bind.setLong("timebucket", event.timeBucket.key)
        bind.setUUID("timestamp", event.timeUuid)
        bind.setString("persistence_id", event.persistenceId)
        session.selectOne(bind)
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
    psUpdateTagView.flatMap { ps =>
      // primary key
      val bind = ps.bind()
      bind.setString("tag_name", tag)
      bind.setLong("timebucket", event.timeBucket.key)
      bind.setUUID("timestamp", event.timeUuid)
      bind.setString("persistence_id", event.persistenceId)
      bind.setLong("tag_pid_sequence_nr", tagPidSequenceNr)

      // event update
      bind.setBytes("event", event.serialized)
      bind.setString("ser_manifest", event.serManifest)
      bind.setInt("ser_id", event.serId)
      bind.setString("event_manifest", event.eventAdapterManifest)

      session.executeWrite(bind)
    }

  private def prepareUpdate(ps: PreparedStatement, s: Serialized, partitionNr: Long): Statement = {
    val bs = ps.bind()

    // primary key
    bs.setString("persistence_id", s.persistenceId)
    bs.setLong("partition_nr", partitionNr)
    bs.setLong("sequence_nr", s.sequenceNr)
    bs.setUUID("timestamp", s.timeUuid)
    bs.setString("timebucket", s.timeBucket.key.toString)

    // fields to update
    bs.setInt("ser_id", s.serId)
    bs.setString("ser_manifest", s.serManifest)
    bs.setString("event_manifest", s.eventAdapterManifest)
    bs.setBytes("event", s.serialized)
    bs.setSet("tags", s.tags.asJava, classOf[String])
    bs
  }
}
