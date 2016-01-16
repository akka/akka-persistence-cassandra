/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

trait CassandraStatements {
  def config: CassandraSnapshotStoreConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        persistence_id text,
        sequence_nr bigint,
        timestamp bigint,
        snapshot blob,
        PRIMARY KEY (persistence_id, sequence_nr))
        WITH CLUSTERING ORDER BY (sequence_nr DESC)
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def writeSnapshot = s"""
      INSERT INTO ${tableName} (persistence_id, sequence_nr, timestamp, snapshot)
      VALUES (?, ?, ?, ?)
    """

  def deleteSnapshot = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  def selectSnapshot = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  def selectSnapshotMetadata(limit: Option[Int] = None) = s"""
      SELECT persistence_id, sequence_nr, timestamp FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr <= ?
        ${limit.map(l => s"LIMIT ${l}").getOrElse("")}
    """

  private def tableName = s"${config.keyspace}.${config.table}"
}
