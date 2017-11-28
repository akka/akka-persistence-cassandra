/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.persistence.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.PreparedStatement

import scala.concurrent.{ ExecutionContext, Future }

trait TaggedPreparedStatements extends CassandraStatements {
  val session: CassandraSession
  implicit val ec: ExecutionContext

  def preparedWriteToTagView: Future[PreparedStatement] = session.prepare(writeTags).map(_.setIdempotent(true))
  def preparedWriteToTagProgress: Future[PreparedStatement] = session.prepare(writeTagProgress).map(_.setIdempotent(true))
  def preparedSelectTagProgress: Future[PreparedStatement] = session.prepare(selectTagProgress).map(_.setIdempotent(true))
  def preparedSelectTagProgressForPersistenceId: Future[PreparedStatement] = session.prepare(selectTagProgressForPersistenceId).map(_.setIdempotent(true))
}
