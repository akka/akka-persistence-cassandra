/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.cassandra.session.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import akka.cassandra.session.{ CassandraSessionSettings, CassandraWriteSettings }
import akka.dispatch.ExecutionContexts
import akka.stream.scaladsl.Flow
import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BatchType, BoundStatement, PreparedStatement }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

/**
 * Scala API to create Cassandra flows.
 */
class CassandraFlow {

  /**
   *
   * @param sessionSettings
   * @param writeSettings
   * @param stmt CQL statement
   * @param statementBinder
   * @tparam T
   * @return
   */
  def createWithPassThrough[T](
      sessionSettings: CassandraSessionSettings,
      writeSettings: CassandraWriteSettings,
      stmt: String,
      statementBinder: (T, PreparedStatement) => BoundStatement): Flow[T, T, NotUsed] =
    Flow
      .setup { (materializer, _) =>
        implicit val executionContext: ExecutionContext = materializer.system.dispatcher
        val session = CassandraSessionRegistry(materializer.system).sessionFor(sessionSettings, executionContext)
        val prepareStatement: Future[T => Future[T]] =
          session.prepare(stmt).map(s => executePrepared(session, s, statementBinder)(_))
        Flow[T].mapAsync(writeSettings.parallelism) { t =>
          prepareStatement.flatMap(executeWriteForElement => executeWriteForElement(t))
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   *
   * @param session
   * @param writeSettings
   * @param stmt
   * @param statementBinder
   * @tparam T
   * @return
   */
  def createWithPassThrough[T](
      session: CassandraSession,
      writeSettings: CassandraWriteSettings,
      stmt: PreparedStatement,
      statementBinder: (T, PreparedStatement) => BoundStatement): Flow[T, T, NotUsed] = {
    val executeWriteForElement: T => Future[T] = executePrepared(session, stmt, statementBinder)
    Flow[T].mapAsync(writeSettings.parallelism)(t => executeWriteForElement(t))
  }

  private def executePrepared[T](
      session: CassandraSession,
      stmt: PreparedStatement,
      statementBinder: (T, PreparedStatement) => BoundStatement)(t: T) = {
    session.executeWrite(statementBinder(t, stmt)).map(_ => t)(ExecutionContexts.sameThreadExecutionContext)
  }

  /**
   * Creates a flow that batches using an unlogged batch. Use this when most of the elements in the stream
   * share the same partition key. Cassandra unlogged batches that share the same partition key will only
   * resolve to one write internally in Cassandra, boosting write performance.
   *
   * Be aware that this stage does NOT preserve the upstream order.
   */
  def createUnloggedBatchWithPassThrough[T, K](
      sessionSettings: CassandraSessionSettings,
      writeSettings: CassandraWriteSettings,
      stmt: String,
      statementBinder: (T, PreparedStatement) => BoundStatement,
      partitionKey: T => K): Flow[T, T, NotUsed] =
    Flow
      .setup { (materializer, _) =>
        implicit val executionContext: ExecutionContext = materializer.system.dispatcher
        val session = CassandraSessionRegistry(materializer.system).sessionFor(sessionSettings, executionContext)
        val stmtFut: Future[PreparedStatement] = session.prepare(stmt)
        Flow[T]
          .groupedWithin(writeSettings.maxBatchSize, writeSettings.maxBatchWait)
          .map(_.groupBy(partitionKey).values.toList)
          .mapConcat(identity)
          .mapAsyncUnordered(writeSettings.parallelism)(list =>
            stmtFut.flatMap { statement =>
              val boundStatements = list.map(t => statementBinder(t, statement))
              val batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED).addAll(boundStatements.asJava)
              session.executeWriteBatch(batchStatement).map(_ => list)(ExecutionContexts.sameThreadExecutionContext)
            })
          .mapConcat(_.toList)
      }
      .mapMaterializedValue(_ => NotUsed)
}
