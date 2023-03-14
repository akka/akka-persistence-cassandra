/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.persistence.cassandra.journal.CassandraJournalStatements
import akka.persistence.cassandra.snapshot.CassandraSnapshotStatements
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class CassandraStatements(val settings: PluginSettings) {

  val journalStatements: CassandraJournalStatements = new CassandraJournalStatements(settings)

  val snapshotStatements: CassandraSnapshotStatements = new CassandraSnapshotStatements(settings.snapshotSettings)

  /**
   * Execute creation of keyspace and tables if that is enabled in config.
   * Avoid calling this from several threads at the same time to
   * reduce the risk of (annoying) "Column family ID mismatch" exception.
   *
   * Exceptions will be logged but will not fail the returned Future.
   */
  def executeAllCreateKeyspaceAndTables(session: CqlSession, log: LoggingAdapter)(
      implicit ec: ExecutionContext): Future[Done] = {
    for {
      _ <- journalStatements.executeCreateKeyspaceAndTables(session, log)
      _ <- snapshotStatements.executeCreateKeyspaceAndTables(session, log)
    } yield Done
  }

}

/**
 * INTERNAL API: caching to avoid repeated check via ColumnDefinitions
 */
@InternalApi private[akka] class ColumnDefinitionCache {
  private def hasColumn(column: String, row: Row, cached: Option[Boolean], updateCache: Boolean => Unit): Boolean = {
    cached match {
      case Some(b) => b
      case None =>
        val b = row.getColumnDefinitions.contains(column)
        updateCache(b)
        b
    }
  }

  @volatile private var _hasMetaColumns: Option[Boolean] = None
  private val updateMetaColumnsCache: Boolean => Unit = b => _hasMetaColumns = Some(b)
  def hasMetaColumns(row: Row): Boolean =
    hasColumn("meta", row, _hasMetaColumns, updateMetaColumnsCache)

  @volatile private var _hasOldTagsColumns: Option[Boolean] = None
  private val updateOldTagsColumnsCache: Boolean => Unit = b => _hasOldTagsColumns = Some(b)
  def hasOldTagsColumns(row: Row): Boolean =
    hasColumn("tag1", row, _hasOldTagsColumns, updateOldTagsColumnsCache)

  @volatile private var _hasTagsColumn: Option[Boolean] = None
  private val updateTagsColumnCache: Boolean => Unit = b => _hasTagsColumn = Some(b)
  def hasTagsColumn(row: Row): Boolean =
    hasColumn("tags", row, _hasTagsColumn, updateTagsColumnCache)

  @volatile private var _hasMessageColumn: Option[Boolean] = None
  private val updateMessageColumnCache: Boolean => Unit = b => _hasMessageColumn = Some(b)
  def hasMessageColumn(row: Row): Boolean =
    hasColumn("message", row, _hasMessageColumn, updateMessageColumnCache)
}
