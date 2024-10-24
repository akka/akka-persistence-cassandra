/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import com.datastax.oss.driver.api.core.cql.PreparedStatement
import scala.concurrent.{ ExecutionContext, Future }

import akka.annotation.InternalApi
import akka.persistence.cassandra.CachedPreparedStatement

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TaggedPreparedStatements(
    statements: CassandraJournalStatements,
    prepare: String => Future[PreparedStatement])(implicit val ec: ExecutionContext) {

  def init(): Unit = {
    writeTagViewWithoutMeta.get()
    writeTagViewWithMeta.get()
    writeTagProgress.get()
    selectTagProgress.get()
    selectTagProgressForPersistenceId.get()
    writeTagScanning.get()
    selectTagScanningForPersistenceId.get()
  }

  val writeTagViewWithoutMeta: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.writeTags(false)))
  val writeTagViewWithMeta: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.writeTags(true)))
  val writeTagProgress: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.writeTagProgress))
  val selectTagProgress: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.selectTagProgress))
  val selectTagProgressForPersistenceId: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.selectTagProgressForPersistenceId))
  val writeTagScanning: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.writeTagScanning))
  val selectTagScanningForPersistenceId: CachedPreparedStatement =
    new CachedPreparedStatement(() => prepare(statements.selectTagScanningForPersistenceId))
}
