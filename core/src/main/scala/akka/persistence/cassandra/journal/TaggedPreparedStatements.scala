/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import com.datastax.oss.driver.api.core.cql.PreparedStatement

import scala.concurrent.{ ExecutionContext, Future }

class TaggedPreparedStatements(statements: CassandraJournalStatements, prepare: String => Future[PreparedStatement])(
    implicit val ec: ExecutionContext) {

  def init(): Unit = {
    WriteTagViewWithoutMeta
    WriteTagViewWithMeta
    WriteTagProgress
    SelectTagProgress
    SelectTagProgressForPersistenceId
    WriteTagScanning
    SelectTagScanningForPersistenceId
  }

  lazy val WriteTagViewWithoutMeta: Future[PreparedStatement] = prepare(statements.writeTags(false))
  lazy val WriteTagViewWithMeta: Future[PreparedStatement] = prepare(statements.writeTags(true))
  lazy val WriteTagProgress: Future[PreparedStatement] = prepare(statements.writeTagProgress)
  lazy val SelectTagProgress: Future[PreparedStatement] = prepare(statements.selectTagProgress)
  lazy val SelectTagProgressForPersistenceId: Future[PreparedStatement] =
    prepare(statements.selectTagProgressForPersistenceId)
  lazy val WriteTagScanning: Future[PreparedStatement] = prepare(statements.writeTagScanning)
  lazy val SelectTagScanningForPersistenceId: Future[PreparedStatement] =
    prepare(statements.selectTagScanningForPersistenceId)
}
