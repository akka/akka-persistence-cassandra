/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import akka.NotUsed
import akka.stream.alpakka.cassandra.CassandraWriteSettings
import akka.stream.alpakka.cassandra.scaladsl
import akka.stream.javadsl.{ Flow, FlowWithContext }
import com.datastax.oss.driver.api.core.cql.{ BoundStatement, PreparedStatement }

/**
 * Java API to create Cassandra flows.
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
      statementBinder: akka.japi.Function2[T, PreparedStatement, BoundStatement]): Flow[T, T, NotUsed] =
    scaladsl.CassandraFlow
      .create(
        session.delegate,
        writeSettings,
        cqlStatement,
        (t, preparedStatement) => statementBinder.apply(t, preparedStatement))
      .asJava

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
      statementBinder: akka.japi.Function2[T, PreparedStatement, BoundStatement])
      : FlowWithContext[T, Ctx, T, Ctx, NotUsed] = {
    scaladsl.CassandraFlow
      .withContext(
        session.delegate,
        writeSettings,
        cqlStatement,
        (t, preparedStatement) => statementBinder.apply(t, preparedStatement))
      .asJava
  }

}
