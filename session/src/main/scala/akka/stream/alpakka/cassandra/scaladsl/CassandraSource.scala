/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.cassandra.session.scaladsl.CassandraSession
import akka.stream.scaladsl.Source
import com.datastax.oss.driver.api.core.cql.{ Row, Statement }

import scala.concurrent.Future

/**
 * Scala API.
 */
object CassandraSource {

  /**
   * Prepare, bind and execute a select statement in one go.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
   */
  def apply(stmt: String, bindValues: AnyRef*)(implicit session: CassandraSession): Source[Row, NotUsed] =
    session.select(stmt, bindValues: _*)

  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def apply(stmt: Statement[_])(implicit session: CassandraSession): Source[Row, NotUsed] =
    session.select(stmt)

  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def fromFuture(stmt: Future[Statement[_]])(implicit session: CassandraSession): Source[Row, NotUsed] =
    session.select(stmt)

}
