/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.annotation.InternalApi
import akka.persistence.cassandra.PluginSettings

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait CassandraReadStatements {

  def settings: PluginSettings
  private def journalSettings = settings.journalSettings

  private def tableName = s"${journalSettings.keyspace}.${journalSettings.table}"
  private def tagViewTableName = s"${journalSettings.keyspace}.tag_views"

  def selectDistinctPersistenceIds =
    s"""
      SELECT DISTINCT persistence_id, partition_nr FROM $tableName
     """

  def selectEventsFromTagViewWithUpperBound =
    s"""
      SELECT * FROM $tagViewTableName WHERE
        tag_name = ?  AND
        timebucket = ? AND
        timestamp > ? AND
        timestamp < ?
        ORDER BY timestamp ASC
     """.stripMargin

  def selectTagSequenceNrs =
    s"""
    SELECT persistence_id, tag_pid_sequence_nr, timestamp
    FROM $tagViewTableName WHERE
    tag_name = ? AND
    timebucket = ? AND
    timestamp > ? AND
    timestamp <= ?"""
}
