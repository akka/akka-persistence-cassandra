/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

private[query] trait CassandraReadStatements {

  def config: CassandraReadJournalConfig

  private def eventsByTagViewName = s"${config.keyspace}.${config.eventsByTagView}"
  private def tableName = s"${config.keyspace}.${config.table}"

  def selectEventsByTag(tagId: Int) = s"""
      SELECT * FROM $eventsByTagViewName$tagId WHERE
        tag$tagId = ? AND
        timebucket = ? AND
        timestamp > ? AND
        timestamp <= ?
        ORDER BY timestamp ASC
        LIMIT ?
    """

  def selectDistinctPersistenceIds = s"""
      SELECT DISTINCT persistence_id, partition_nr FROM $tableName
     """
}
