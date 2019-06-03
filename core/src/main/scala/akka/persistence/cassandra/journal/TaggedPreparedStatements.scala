/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import com.datastax.driver.core.PreparedStatement

import scala.concurrent.{ ExecutionContext, Future }

trait TaggedPreparedStatements extends CassandraStatements {
  private[akka] val session: CassandraSession
  private[akka] implicit val ec: ExecutionContext

  def preparedWriteToTagViewWithoutMeta: Future[PreparedStatement] =
    session.prepare(writeTags(false)).map(_.setIdempotent(true))
  def preparedWriteToTagViewWithMeta: Future[PreparedStatement] =
    session.prepare(writeTags(true)).map(_.setIdempotent(true))
  def preparedWriteToTagProgress: Future[PreparedStatement] =
    session.prepare(writeTagProgress).map(_.setIdempotent(true))
  def preparedSelectTagProgress: Future[PreparedStatement] =
    session.prepare(selectTagProgress).map(_.setIdempotent(true))
  def preparedSelectTagProgressForPersistenceId: Future[PreparedStatement] =
    session.prepare(selectTagProgressForPersistenceId).map(_.setIdempotent(true))
  def preparedWriteTagScanning: Future[PreparedStatement] =
    session.prepare(writeTagScanning).map(_.setIdempotent(true))
  def preparedSelectTagScanningForPersistenceId: Future[PreparedStatement] =
    session.prepare(selectTagScanningForPersistenceId).map(_.setIdempotent(true))
}
