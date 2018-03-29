/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.Done
import com.datastax.driver.core.Session
import akka.persistence.cassandra.session.scaladsl.CassandraSession

trait CassandraStatements {
  def snapshotConfig: CassandraSnapshotStoreConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${snapshotConfig.keyspace}
      WITH REPLICATION = { 'class' : ${snapshotConfig.replicationStrategy} }
    """

  // snapshot_data is the serialized snapshot payload
  // snapshot is for backwards compatibility and not used for new rows
  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        persistence_id text,
        sequence_nr bigint,
        timestamp bigint,
        ser_id int,
        ser_manifest text,
        snapshot_data blob,
        snapshot blob,
        meta_ser_id int,
        meta_ser_manifest text,
        meta blob,
        PRIMARY KEY (persistence_id, sequence_nr))
        WITH CLUSTERING ORDER BY (sequence_nr DESC) AND gc_grace_seconds =${snapshotConfig.gcGraceSeconds}
        AND compaction = ${snapshotConfig.tableCompactionStrategy.asCQL}
    """

  def writeSnapshot(withMeta: Boolean) = s"""
      INSERT INTO ${tableName} (persistence_id, sequence_nr, timestamp, ser_manifest, ser_id, snapshot_data
      ${if (withMeta) ", meta_ser_id, meta_ser_manifest, meta" else ""})
      VALUES (?, ?, ?, ?, ?, ? ${if (withMeta) ", ?, ?, ?" else ""})
    """

  def deleteSnapshot = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  def deleteAllSnapshotForPersistenceId = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ?
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

  private def tableName = s"${snapshotConfig.keyspace}.${snapshotConfig.table}"

  /**
   * Execute creation of keyspace and tables is limited to one thread at a time to
   * reduce the risk of (annoying) "Column family ID mismatch" exception
   * when write and read-side plugins are started at the same time.
   * Those statements are retried, because that could happen across different
   * nodes also but serializing those statements gives a better "experience".
   */
  def executeCreateKeyspaceAndTables(session: Session, config: CassandraSnapshotStoreConfig)(implicit ec: ExecutionContext): Future[Done] = {
    import akka.persistence.cassandra.listenableFutureToFuture

    def create(): Future[Done] = {
      val keyspace: Future[Done] =
        if (config.keyspaceAutoCreate) session.executeAsync(createKeyspace).map(_ => Done)
        else Future.successful(Done)

      if (config.tablesAutoCreate) keyspace.flatMap(_ => session.executeAsync(createTable)).map(_ => Done)
      else keyspace
    }

    CassandraSession.serializedExecution(
      recur = () => executeCreateKeyspaceAndTables(session, config),
      exec = () => create()
    )
  }
}
