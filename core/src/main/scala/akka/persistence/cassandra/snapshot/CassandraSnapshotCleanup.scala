/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }

import akka.Done
import akka.persistence.SnapshotMetadata
import akka.persistence.cassandra.journal.FixedRetryPolicy
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.policies.LoggingRetryPolicy

import scala.concurrent.{ ExecutionContext, Future }

trait CassandraSnapshotCleanup extends CassandraStatements {

  def snapshotConfig: CassandraSnapshotStoreConfig
  def session: CassandraSession
  private[akka] implicit val ec: ExecutionContext

  private lazy val deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(snapshotConfig.deleteRetries))

  def preparedDeleteSnapshot = session.prepare(deleteSnapshot).map(
    _.setConsistencyLevel(snapshotConfig.writeConsistency).setIdempotent(true).setRetryPolicy(deleteRetryPolicy))

  def preparedDeleteAllSnapshotsForPid = session.prepare(deleteAllSnapshotForPersistenceId).map(
    _.setConsistencyLevel(snapshotConfig.writeConsistency).setIdempotent(true).setRetryPolicy(deleteRetryPolicy))

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val boundDeleteSnapshot = preparedDeleteSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundDeleteSnapshot.flatMap(session.executeWrite(_)).map(_ => ())
  }

  def deleteAllForPersistenceId(pid: String): Future[Done] = {
    val bound = preparedDeleteAllSnapshotsForPid.map(_.bind(pid))
    bound.flatMap(session.executeWrite(_)).map(_ => Done)
  }
}
