/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import scala.concurrent.{ ExecutionContext, Future }
import akka.Done
import com.datastax.driver.core.Session
import akka.persistence.cassandra._

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
        ser_id int,
        ser_manifest text,
        snapshot_data blob,
        snapshot blob,
        PRIMARY KEY (persistence_id, sequence_nr))
        WITH CLUSTERING ORDER BY (sequence_nr DESC)
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def writeSnapshot = s"""
      INSERT INTO ${tableName} (persistence_id, sequence_nr, timestamp, ser_manifest, ser_id, snapshot_data, snapshot)
      VALUES (?, ?, ?, ?, ?, ?, ?)
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

  /**
   * Execute creation of keyspace and tables is limited to one thread at a time to
   * reduce the risk of (annoying) "Column family ID mismatch" exception
   * when write and read-side plugins are started at the same time.
   * Those statements are retried, because that could happen across different
   * nodes also but serializing those statements gives a better "experience".
   */
  def executeCreateKeyspaceAndTables(session: Session, config: CassandraSnapshotStoreConfig)(implicit ec: ExecutionContext): Future[Done] = {

    def create(): Future[Done] = {
      val keyspace: Future[Done] =
        if (config.keyspaceAutoCreate) session.executeAsync(createKeyspace).asScala.map(_ => Done)
        else Future.successful(Done)

      if (config.tablesAutoCreate) keyspace.flatMap(_ => session.executeAsync(createTable).asScala).map(_ => Done)
      else keyspace
    }

    CassandraSession.serializedExecution(
      recur = () => executeCreateKeyspaceAndTables(session, config),
      exec = () => create()
    )
  }
}
