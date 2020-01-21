/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }

import akka.Done
import akka.cassandra.session.scaladsl.CassandraSession
import akka.persistence.SnapshotMetadata
import com.datastax.oss.driver.api.core.cql.PreparedStatement

import scala.concurrent.{ ExecutionContext, Future }

trait CassandraSnapshotCleanup extends CassandraSnapshotStatements {

  def snapshotSettings: SnapshotSettings
  def session: CassandraSession
  private[akka] implicit val ec: ExecutionContext

  def preparedDeleteSnapshot: Future[PreparedStatement] =
    session.prepare(deleteSnapshot)

  def preparedDeleteAllSnapshotsForPid: Future[PreparedStatement] =
    session.prepare(deleteAllSnapshotForPersistenceId)

  def preparedDeleteAllSnapshotsForPidAndSequenceNrBetween: Future[PreparedStatement] =
    session.prepare(deleteAllSnapshotForPersistenceIdAndSequenceNrBetween)

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val boundDeleteSnapshot = preparedDeleteSnapshot.map(_.bind(metadata.persistenceId, metadata.sequenceNr: JLong))
    boundDeleteSnapshot.flatMap(session.executeWrite(_)).map(_ => ())
  }

  def deleteAllForPersistenceId(pid: String): Future[Done] = {
    val bound = preparedDeleteAllSnapshotsForPid.map(_.bind(pid))
    bound.flatMap(session.executeWrite(_)).map(_ => Done)
  }
}
