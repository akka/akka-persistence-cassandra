/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.cassandra.session.FutureDone
import akka.persistence.cassandra.indent
import com.datastax.oss.driver.api.core.CqlSession

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraSnapshotStatements(snapshotSettings: SnapshotSettings) {

  def createKeyspace =
    s"""
    | CREATE KEYSPACE IF NOT EXISTS ${snapshotSettings.keyspace}
    | WITH REPLICATION = { 'class' : ${snapshotSettings.replicationStrategy} }
    """.stripMargin.trim

  // snapshot_data is the serialized snapshot payload
  // snapshot is for backwards compatibility and not used for new rows
  def createTable =
    s"""
    |CREATE TABLE IF NOT EXISTS ${tableName} (
    |  persistence_id text,
    |  sequence_nr bigint,
    |  timestamp bigint,
    |  ser_id int,
    |  ser_manifest text,
    |  snapshot_data blob,
    |  snapshot blob,
    |  meta_ser_id int,
    |  meta_ser_manifest text,
    |  meta blob,
    |  PRIMARY KEY (persistence_id, sequence_nr))
    |  WITH CLUSTERING ORDER BY (sequence_nr DESC) AND gc_grace_seconds =${snapshotSettings.gcGraceSeconds}
    |  AND compaction = ${indent(snapshotSettings.tableCompactionStrategy.asCQL, "    ")}
    """.stripMargin.trim

  def writeSnapshot(withMeta: Boolean): String = s"""
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

  def deleteAllSnapshotForPersistenceIdAndSequenceNrBetween = s"""
    DELETE FROM ${tableName}
    WHERE persistence_id = ?
    AND sequence_nr >= ?
    AND sequence_nr <= ?
  """

  def selectSnapshot = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  def selectSnapshotMetadata(limit: Option[Int] = None) = s"""
      SELECT persistence_id, sequence_nr, timestamp FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr <= ? AND
        sequence_nr >= ?
        ${limit.map(l => s"LIMIT ${l}").getOrElse("")}
    """

  private def tableName = s"${snapshotSettings.keyspace}.${snapshotSettings.table}"

  /**
   * Execute creation of keyspace and tables if that is enabled in config.
   * Avoid calling this from several threads at the same time to
   * reduce the risk of (annoying) "Column family ID mismatch" exception.
   */
  def executeCreateKeyspaceAndTables(session: CqlSession)(implicit ec: ExecutionContext): Future[Done] = {
    def keyspace: Future[Done] =
      if (snapshotSettings.keyspaceAutoCreate)
        session.executeAsync(createKeyspace).toScala.map(_ => Done)
      else FutureDone

    if (snapshotSettings.tablesAutoCreate) {
      // reason for setSchemaMetadataEnabled is that it speed up tests by multiple factors
      session.setSchemaMetadataEnabled(false)
      val result = for {
        _ <- keyspace
        _ <- session.executeAsync(createTable).toScala
      } yield {
        session.setSchemaMetadataEnabled(null)
        Done
      }
      result.failed.foreach(_ => session.setSchemaMetadataEnabled(null))
      result
    } else keyspace
  }
}
