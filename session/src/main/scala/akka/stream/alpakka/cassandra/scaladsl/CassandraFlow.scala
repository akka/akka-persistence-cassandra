/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.cassandra.CassandraWriteSettings
import akka.stream.scaladsl.{ Flow, FlowWithContext }
import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BatchType, BoundStatement, PreparedStatement }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

/**
 * Scala API to create Cassandra flows.
 */
object CassandraFlow {

  /**
   * A flow writing to Cassandra for every stream element.
   * The element is emitted unchanged.
   *
   * @param session Cassandra session from `CassandraSessionRegistry`
   * @param writeSettings settings to configure the write operation
   * @param cqlStatement raw CQL statement
   * @param statementBinder function to bind data from the stream element to the prepared statement
   * @tparam T stream element type
   */
  def create[T](
      session: CassandraSession,
      writeSettings: CassandraWriteSettings,
      cqlStatement: String,
      statementBinder: (T, PreparedStatement) => BoundStatement): Flow[T, T, NotUsed] = {
    Flow
      .setup { (materializer, _) =>
        implicit val executionContext: ExecutionContext = materializer.system.dispatcher
        val prepareStatement = session.prepare(cqlStatement)
        Flow[T].mapAsync(writeSettings.parallelism) { element =>
          prepareStatement
            .flatMap { preparedStatement =>
              session.executeWrite(statementBinder(element, preparedStatement))
            }
            .map(_ => element)(ExecutionContexts.sameThreadExecutionContext)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  /**
   * A flow writing to Cassandra for every stream element, passing context along.
   * The element and context are emitted unchanged.
   *
   * @param session Cassandra session from `CassandraSessionRegistry`
   * @param writeSettings settings to configure the write operation
   * @param cqlStatement raw CQL statement
   * @param statementBinder function to bind data from the stream element to the prepared statement
   * @tparam T stream element type
   * @tparam Ctx context type
   */
  def withContext[T, Ctx](
      session: CassandraSession,
      writeSettings: CassandraWriteSettings,
      cqlStatement: String,
      statementBinder: (T, PreparedStatement) => BoundStatement): FlowWithContext[T, Ctx, T, Ctx, NotUsed] = {
    FlowWithContext.fromTuples(
      Flow
        .setup { (materializer, _) =>
          implicit val executionContext: ExecutionContext = materializer.system.dispatcher
          val prepareStatement = session.prepare(cqlStatement)
          Flow[(T, Ctx)].mapAsync(writeSettings.parallelism) {
            case tuple @ (element, _) =>
              prepareStatement
                .flatMap { preparedStatement =>
                  session.executeWrite(statementBinder(element, preparedStatement))
                }
                .map(_ => tuple)(ExecutionContexts.sameThreadExecutionContext)
          }
        }
        .mapMaterializedValue(_ => NotUsed))
  }

  /**
   * Creates a flow that batches using an unlogged batch. Use this when most of the elements in the stream
   * share the same partition key. Cassandra unlogged batches that share the same partition key will only
   * resolve to one write internally in Cassandra, boosting write performance.
   *
   * Be aware that this stage does NOT preserve the upstream order.
   */
  def createUnloggedBatch[T, K](
      session: CassandraSession,
      writeSettings: CassandraWriteSettings,
      cqlStatement: String,
      statementBinder: (T, PreparedStatement) => BoundStatement,
      partitionKey: T => K): Flow[T, T, NotUsed] =
    Flow
      .setup { (materializer, _) =>
        implicit val executionContext: ExecutionContext = materializer.system.dispatcher
        val prepareStatement: Future[PreparedStatement] = session.prepare(cqlStatement)
        Flow[T]
          .groupedWithin(writeSettings.maxBatchSize, writeSettings.maxBatchWait)
          .map(_.groupBy(partitionKey).values.toList)
          .mapConcat(identity)
          .mapAsyncUnordered(writeSettings.parallelism)(list =>
            prepareStatement.flatMap { preparedStatement =>
              val boundStatements = list.map(t => statementBinder(t, preparedStatement))
              val batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED).addAll(boundStatements.asJava)
              session.executeWriteBatch(batchStatement).map(_ => list)(ExecutionContexts.sameThreadExecutionContext)
            })
          .mapConcat(_.toList)
      }
      .mapMaterializedValue(_ => NotUsed)
}
