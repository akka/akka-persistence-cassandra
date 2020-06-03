/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.indent
import com.datastax.oss.driver.api.core.CqlSession
import akka.persistence.cassandra.FutureDone

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraJournalStatements(settings: PluginSettings) {
  private val journalSettings = settings.journalSettings
  private val eventsByTagSettings = settings.eventsByTagSettings

  def createKeyspace =
    s"""
     |CREATE KEYSPACE IF NOT EXISTS ${journalSettings.keyspace}
     |WITH REPLICATION = { 'class' : ${journalSettings.replicationStrategy} }
    """.stripMargin.trim

  // The event's serialization manifest is stored in ser_manifest and the
  // PersistentRepr.manifest is stored in event_manifest (sorry for naming confusion).
  // PersistentRepr.manifest is used by the event adapters (in akka-persistence).
  // ser_manifest together with ser_id is used for the serialization of the event (payload).
  def createTable =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableName (
      |  persistence_id text,
      |  partition_nr bigint,
      |  sequence_nr bigint,
      |  timestamp timeuuid,
      |  timebucket text,
      |  writer_uuid text,
      |  ser_id int,
      |  ser_manifest text,
      |  event_manifest text,
      |  event blob,
      |  meta_ser_id int,
      |  meta_ser_manifest text,
      |  meta blob,
      |  tags set<text>,
      |  PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp))
      |  WITH gc_grace_seconds =${journalSettings.gcGraceSeconds}
      |  AND compaction = ${indent(journalSettings.tableCompactionStrategy.asCQL, "    ")}
    """.stripMargin.trim

  def createTagsTable =
    s"""
      |CREATE TABLE IF NOT EXISTS $tagTableName (
      |  tag_name text,
      |  persistence_id text,
      |  sequence_nr bigint,
      |  timebucket bigint,
      |  timestamp timeuuid,
      |  tag_pid_sequence_nr bigint,
      |  writer_uuid text,
      |  ser_id int,
      |  ser_manifest text,
      |  event_manifest text,
      |  event blob,
      |  meta_ser_id int,
      |  meta_ser_manifest text,
      |  meta blob,
      |  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr))
      |  WITH gc_grace_seconds =${eventsByTagSettings.tagTable.gcGraceSeconds}
      |  AND compaction = ${indent(eventsByTagSettings.tagTable.compactionStrategy.asCQL, "    ")}
      |  ${if (eventsByTagSettings.tagTable.ttl.isDefined)
         "AND default_time_to_live = " + eventsByTagSettings.tagTable.ttl.get.toSeconds
       else ""}
    """.stripMargin.trim

  def createTagsProgressTable: String =
    s"""
     |CREATE TABLE IF NOT EXISTS $tagProgressTableName(
     |  persistence_id text,
     |  tag text,
     |  sequence_nr bigint,
     |  tag_pid_sequence_nr bigint,
     |  offset timeuuid,
     |  PRIMARY KEY (persistence_id, tag))
     """.stripMargin.trim

  def createTagScanningTable: String =
    s"""
     |CREATE TABLE IF NOT EXISTS $tagScanningTableName(
     |  persistence_id text,
     |  sequence_nr bigint,
     |  PRIMARY KEY (persistence_id))
     """.stripMargin.trim

  def createMetadataTable: String =
    s"""
     |CREATE TABLE IF NOT EXISTS $metadataTableName(
     |  persistence_id text PRIMARY KEY,
     |  deleted_to bigint,
     |  properties map<text,text>)
    """.stripMargin.trim

  def createAllPersistenceIdsTable: String =
    s"""
     |CREATE TABLE IF NOT EXISTS $allPersistenceIdsTableName(
     |  persistence_id text PRIMARY KEY)
    """.stripMargin.trim

  def writeMessage(withMeta: Boolean) =
    s"""
      INSERT INTO $tableName (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event,
        ${if (withMeta) "meta_ser_id, meta_ser_manifest, meta," else ""}
        tags)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ${if (withMeta) "?, ?, ?, " else ""} ?)
    """

  // could just use the write tags Statement if we're going to update all the fields.
  // Fields that are not updated atm: writer_uuid and metadata fields
  def updateMessagePayloadAndTags: String =
    s"""
       UPDATE $tableName
       SET
        event = ?,
        ser_manifest = ?,
        ser_id = ?,
        event_manifest = ?,
        tags = ?
       WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ? AND
        timestamp = ?
     """

  def writeTags(withMeta: Boolean): String =
    s"""
       INSERT INTO $tagTableName(
        tag_name,
        timebucket,
        timestamp,
        tag_pid_sequence_nr,
        event,
        event_manifest,
        persistence_id,
        sequence_nr,
        ser_id,
        ser_manifest,
        writer_uuid
        ${if (withMeta) ", meta_ser_id, meta_ser_manifest, meta" else ""}
        ) VALUES (?,?,?,?,?,?,?,?,?,?,
        ${if (withMeta) "?, ?, ?," else ""}
        ?)
     """

  def deleteTag: String =
    s"""
       DELETE FROM $tagTableName WHERE tag_name = ? and timebucket = ? and timestamp = ? and persistence_id = ? and tag_pid_sequence_nr = ?
    """

  def deleteTagProgress: String =
    s"""
       DELETE FROM $tagProgressTableName WHERE persistence_id = ? and tag = ?
     """

  def deleteTagScanning: String =
    s"""
       DELETE FROM $tagScanningTableName WHERE persistence_id = ?
     """

  def truncateTagViews: String =
    s"TRUNCATE $tagTableName"
  def truncateTagProgress: String =
    s"TRUNCATE $tagProgressTableName"
  def truncateTagScanning: String =
    s"TRUNCATE $tagScanningTableName"
  def selectAllTagProgress: String =
    s"""SELECT tag FROM $tagProgressTableName"""

  def updateMessagePayloadInTagView =
    s"""
       UPDATE $tagTableName
       SET
        event = ?,
        ser_manifest = ?,
        ser_id = ?,
        event_manifest = ?
       WHERE
        tag_name = ? AND
        timebucket = ? AND
        timestamp = ? AND
        persistence_id = ? AND
        tag_pid_sequence_nr = ?
     """

  def selectTagPidSequenceNr =
    s"""
       SELECT tag_pid_sequence_nr
       FROM $tagTableName WHERE
       tag_name = ? AND
       timebucket = ? AND
       timestamp = ? AND
       persistence_id = ?
     """.stripMargin

  def writeTagProgress =
    s"""
       INSERT INTO $tagProgressTableName(
        persistence_id,
        tag,
        sequence_nr,
        tag_pid_sequence_nr,
        offset) VALUES (?, ?, ?, ?, ?)
     """

  def selectTagProgress =
    s"""
       SELECT * from $tagProgressTableName WHERE
       persistence_id = ? AND
       tag = ?
     """

  def selectTagProgressForPersistenceId =
    s"""
       SELECT * from $tagProgressTableName WHERE
       persistence_id = ?
     """

  def writeTagScanning =
    s"""
       INSERT INTO $tagScanningTableName(
         persistence_id, sequence_nr) VALUES (?, ?)
     """

  def selectTagScanningForPersistenceId =
    s"""
       SELECT sequence_nr from $tagScanningTableName WHERE
       persistence_id = ?
     """

  def deleteMessage =
    s"""
      DELETE FROM $tableName WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def deleteMessages(cassandra2xCompat: Boolean) =
    if (cassandra2xCompat)
      s"""
      DELETE FROM $tableName WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """
    else
      s"""
      DELETE FROM $tableName WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= 0 AND
        sequence_nr <= ?
    """

  def selectMessages =
    s"""
      SELECT * FROM $tableName WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """

  def selectHighestSequenceNr =
    s"""
     SELECT sequence_nr FROM $tableName WHERE
       persistence_id = ? AND
       partition_nr = ?
       ORDER BY sequence_nr
       DESC LIMIT 1
   """

  def selectDeletedTo =
    s"""
      SELECT deleted_to FROM $metadataTableName WHERE
        persistence_id = ?
    """

  def insertDeletedTo =
    s"""
      INSERT INTO $metadataTableName (persistence_id, deleted_to)
      VALUES ( ?, ? )
    """

  def deleteDeletedTo =
    s"""
      DELETE FROM $metadataTableName where persistence_id = ?
    """

  def insertIntoAllPersistenceIds =
    s"""
      INSERT INTO $allPersistenceIdsTableName (persistence_id)
      VALUES ( ? )
    """

  def deleteFromAllPersistenceIds =
    s"""
      DELETE FROM $allPersistenceIdsTableName where persistence_id = ?
    """

  protected def tableName = s"${journalSettings.keyspace}.${journalSettings.table}"
  private def tagTableName = s"${journalSettings.keyspace}.${eventsByTagSettings.tagTable.name}"
  private def tagProgressTableName = s"${journalSettings.keyspace}.tag_write_progress"
  private def tagScanningTableName = s"${journalSettings.keyspace}.tag_scanning"
  private def metadataTableName = s"${journalSettings.keyspace}.${journalSettings.metadataTable}"
  private def allPersistenceIdsTableName = s"${journalSettings.keyspace}.${journalSettings.allPersistenceIdsTable}"

  /**
   * Execute creation of keyspace and tables if that is enabled in config.
   * Avoid calling this from several threads at the same time to
   * reduce the risk of (annoying) "Column family ID mismatch" exception.
   *
   * Exceptions will be logged but will not fail the returned Future.
   */
  def executeCreateKeyspaceAndTables(session: CqlSession, log: LoggingAdapter)(
      implicit ec: ExecutionContext): Future[Done] = {

    def tagStatements: Future[Done] =
      if (eventsByTagSettings.eventsByTagEnabled) {
        for {
          _ <- session.executeAsync(createTagsTable).toScala
          _ <- session.executeAsync(createTagsProgressTable).toScala
          _ <- session.executeAsync(createTagScanningTable).toScala
        } yield Done
      } else FutureDone

    def keyspace: Future[Done] =
      if (journalSettings.keyspaceAutoCreate)
        session.executeAsync(createKeyspace).toScala.map(_ => Done)
      else FutureDone

    val done = if (journalSettings.tablesAutoCreate) {
      // reason for setSchemaMetadataEnabled is that it speed up tests by multiple factors
      session.setSchemaMetadataEnabled(false)
      val result = for {
        _ <- keyspace
        _ <- session.executeAsync(createTable).toScala
        _ <- session.executeAsync(createMetadataTable).toScala
        _ <- {
          if (settings.journalSettings.supportAllPersistenceIds)
            session.executeAsync(createAllPersistenceIdsTable).toScala
          else
            FutureDone
        }
        _ <- tagStatements
      } yield {
        session.setSchemaMetadataEnabled(null)
        Done
      }
      result.recoverWith {
        case e =>
          log.warning("Failed to create journal keyspace and tables: {}", e)
          session.setSchemaMetadataEnabled(null)
          FutureDone
      }
    } else {
      keyspace.recoverWith {
        case e =>
          log.warning("Failed to create journal keyspace: {}", e)
          FutureDone
      }
    }

    done
  }

}
