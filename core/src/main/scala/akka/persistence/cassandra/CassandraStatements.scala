/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournalStatements
import akka.persistence.cassandra.snapshot.CassandraSnapshotStatements
import com.datastax.oss.driver.api.core.CqlSession

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class CassandraStatements(val settings: PluginSettings) {

  val journalStatements: CassandraJournalStatements = new CassandraJournalStatements(settings)

  val snapshotStatements: CassandraSnapshotStatements = new CassandraSnapshotStatements(settings.snapshotSettings)

  def executeAllCreateKeyspaceAndTables(session: CqlSession)(implicit ec: ExecutionContext): Future[Done] = {
    for {
      _ <- journalStatements.executeCreateKeyspaceAndTables(session)
      _ <- snapshotStatements.executeCreateKeyspaceAndTables(session)
    } yield Done
  }

}
