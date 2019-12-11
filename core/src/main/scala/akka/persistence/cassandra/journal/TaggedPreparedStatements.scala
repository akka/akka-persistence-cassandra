/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.cassandra.session.scaladsl.CassandraSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement

import scala.concurrent.{ ExecutionContext, Future }

trait TaggedPreparedStatements extends CassandraStatements {
  private[akka] val session: CassandraSession
  private[akka] implicit val ec: ExecutionContext

  def preparedWriteToTagViewWithoutMeta: Future[PreparedStatement] = session.prepare(writeTags(false))
  def preparedWriteToTagViewWithMeta: Future[PreparedStatement] = session.prepare(writeTags(true))
  def preparedWriteToTagProgress: Future[PreparedStatement] = session.prepare(writeTagProgress)
  def preparedSelectTagProgress: Future[PreparedStatement] = session.prepare(selectTagProgress)
  def preparedSelectTagProgressForPersistenceId: Future[PreparedStatement] =
    session.prepare(selectTagProgressForPersistenceId)
  def preparedWriteTagScanning: Future[PreparedStatement] = session.prepare(writeTagScanning)
  def preparedSelectTagScanningForPersistenceId: Future[PreparedStatement] =
    session.prepare(selectTagScanningForPersistenceId)
}
