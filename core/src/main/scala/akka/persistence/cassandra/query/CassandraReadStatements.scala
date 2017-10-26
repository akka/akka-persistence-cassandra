/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait CassandraReadStatements {

  def config: CassandraReadJournalConfig

  private def tableName = s"${config.keyspace}.${config.table}"
  private def tagViewTableName = s"${config.keyspace}.tag_views"

  def selectDistinctPersistenceIds = s"""
      SELECT DISTINCT persistence_id, partition_nr FROM $tableName
     """

  def selectEventsFromTagView =
    s"""
      SELECT * FROM $tagViewTableName WHERE
        tag_name = ?  AND
        timebucket = ? AND
        timestamp > ?
        ORDER BY timestamp ASC
     """.stripMargin

  def selectEventsFromTagViewWithUpperBound =
    s"""
      SELECT * FROM $tagViewTableName WHERE
        tag_name = ?  AND
        timebucket = ? AND
        timestamp > ? AND
        timestamp < ?
     """.stripMargin
}
