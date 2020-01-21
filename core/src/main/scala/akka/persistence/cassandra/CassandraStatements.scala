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
import akka.persistence.cassandra.snapshot.SnapshotSettings
import com.datastax.oss.driver.api.core.CqlSession

@InternalApi
private[akka] trait CassandraStatements {
  private[akka] def settings: PluginSettings

  val journalStatements: CassandraJournalStatements = new CassandraJournalStatements {
    override def settings: PluginSettings = CassandraStatements.this.settings
  }

  val snapshotStatements: CassandraSnapshotStatements = new CassandraSnapshotStatements {
    override def snapshotSettings: SnapshotSettings = settings.snapshotSettings
  }

  def executeAllCreateKeyspaceAndTables(session: CqlSession)(implicit ec: ExecutionContext): Future[Done] = {
    for {
      _ <- journalStatements.executeCreateKeyspaceAndTables(session)
      _ <- snapshotStatements.executeCreateKeyspaceAndTables(session)
    } yield Done
  }

}
