/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.javadsl.Source
import com.datastax.oss.driver.api.core.cql.{ Row, Statement }

import scala.annotation.varargs

/**
 * Java API.
 */
object CassandraSource {

  /**
   * Prepare, bind and execute a select statement in one go.
   */
  @varargs
  def create(session: CassandraSession, stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] =
    session.select(stmt, bindValues: _*)

  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def create(session: CassandraSession, stmt: Statement[_]): Source[Row, NotUsed] =
    session.select(stmt)

  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def fromCompletionStage(session: CassandraSession, stmt: CompletionStage[Statement[_]]): Source[Row, NotUsed] =
    session.select(stmt)

}
