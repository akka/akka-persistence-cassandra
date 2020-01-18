/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraJournalStatements
import akka.persistence.cassandra.snapshot.CassandraSnapshotStatements
import akka.persistence.cassandra.snapshot.CassandraSnapshotStoreConfig
import com.datastax.oss.driver.api.core.CqlSession

@InternalApi
private[akka] trait CassandraStatements {
  def config: CassandraJournalConfig // FIXME rename to journalConfig, or create one single CassandraSettings holding everything
  def snapshotConfig: CassandraSnapshotStoreConfig

  val journalStatements: CassandraJournalStatements = new CassandraJournalStatements {
    override def config: CassandraJournalConfig = CassandraStatements.this.config
  }

  val snapshotStatements: CassandraSnapshotStatements = new CassandraSnapshotStatements {
    override def snapshotConfig: CassandraSnapshotStoreConfig = CassandraStatements.this.snapshotConfig
  }

  def executeAllCreateKeyspaceAndTables(session: CqlSession)(implicit ec: ExecutionContext): Future[Done] = {
    for {
      _ <- journalStatements.executeCreateKeyspaceAndTables(session)
      _ <- snapshotStatements.executeCreateKeyspaceAndTables(session)
    } yield Done
  }

}
