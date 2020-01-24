/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.NotUsed
import akka.cassandra.session.CassandraSessionSettings
import akka.cassandra.session.scaladsl.{ CassandraSession, CassandraSessionRegistry }
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
  def apply(settings: CassandraSessionSettings, stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] =
    Source
      .setup { (materializer, _) =>
        val executionContext = materializer.system.dispatcher
        val session = CassandraSessionRegistry(materializer.system).sessionFor(settings, executionContext)
        session.select(stmt, bindValues: _*)
      }
      .mapMaterializedValue(_ => NotUsed)

  // TODO does this make sense? The user needs to gain access to the session to create Statement[_]
  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def apply(settings: CassandraSessionSettings, stmt: Statement[_]): Source[Row, NotUsed] =
    Source
      .setup { (materializer, _) =>
        val executionContext = materializer.system.dispatcher
        val session = CassandraSessionRegistry(materializer.system).sessionFor(settings, executionContext)
        session.select(stmt)
      }
      .mapMaterializedValue(_ => NotUsed)

  // TODO does this make sense?
  /**
   * Create a [[akka.stream.scaladsl.Source Source]] from a given statement.
   */
  def fromFuture(session: CassandraSession, stmt: Future[Statement[_]]): Source[Row, NotUsed] =
    session.select(stmt)

}
