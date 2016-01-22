/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import com.datastax.driver.core.Session

private object CassandraStatements {
  val createKeyspaceAndTablesLock = new Object
}

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createConfigTable = s"""
      CREATE TABLE IF NOT EXISTS ${configTableName} (
        property text primary key, value text)
     """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        used boolean static,
        persistence_id text,
        partition_nr bigint,
        sequence_nr bigint,
        timestamp timeuuid,
        timebucket text,
        message blob,
        tag1 text,
        tag2 text,
        tag3 text,
        PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
        WITH gc_grace_seconds =${config.gc_grace_seconds}
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def createMetatdataTable = s"""
      CREATE TABLE IF NOT EXISTS ${metadataTableName}(
        persistence_id text PRIMARY KEY,
        deleted_to bigint,
        properties map<text,text>)
   """

  def createEventsByTagMaterializedView(tagId: Int) = s"""
      CREATE MATERIALIZED VIEW IF NOT EXISTS $eventsByTagViewName$tagId AS
         SELECT tag$tagId, timebucket, timestamp, persistence_id, partition_nr, sequence_nr, message
         FROM $tableName
         WHERE persistence_id IS NOT NULL AND partition_nr IS NOT NULL AND sequence_nr IS NOT NULL
           AND tag$tagId IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
         PRIMARY KEY ((tag$tagId, timebucket), timestamp, persistence_id, partition_nr, sequence_nr)
         WITH CLUSTERING ORDER BY (timestamp ASC)
      """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, tag1, tag2, tag3, message, used)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true)
    """

  def deleteMessage = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """

  def selectInUse = s"""
     SELECT used from ${tableName} WHERE
      persistence_id = ? AND
      partition_nr = ?
   """
  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?) IF NOT EXISTS
    """

  def selectHighestSequenceNr = s"""
     SELECT sequence_nr, used FROM ${tableName} WHERE
       persistence_id = ? AND
       partition_nr = ?
       ORDER BY sequence_nr
       DESC LIMIT 1
   """

  def selectDeletedTo = s"""
      SELECT deleted_to FROM ${metadataTableName} WHERE
        persistence_id = ?
    """

  def insertDeletedTo = s"""
      INSERT INTO ${metadataTableName} (persistence_id, deleted_to)
      VALUES ( ?, ? )
    """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (persistence_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
  private def eventsByTagViewName = s"${config.keyspace}.${config.eventsByTagView}"

  /**
   * Execute creation of keyspace and tables in synchronized block to
   * reduce the risk of (annoying) "Column family ID mismatch" exception
   * when write and read-side plugins are started at the same time.
   * Those statements are retried, because that could happen across different
   * nodes also but synchronizing those statements gives a better "experience".
   *
   * The materialized view for eventsByTag query is not created if `maxTagId` is 0.
   */
  def executeCreateKeyspaceAndTables(session: Session, keyspaceAutoCreate: Boolean, maxTagId: Int): Unit =
    CassandraStatements.createKeyspaceAndTablesLock.synchronized {
      if (keyspaceAutoCreate)
        session.execute(createKeyspace)
      session.execute(createTable)
      session.execute(createMetatdataTable)
      session.execute(createConfigTable)
      for (tagId <- 1 to maxTagId)
        session.execute(createEventsByTagMaterializedView(tagId))
    }
}
