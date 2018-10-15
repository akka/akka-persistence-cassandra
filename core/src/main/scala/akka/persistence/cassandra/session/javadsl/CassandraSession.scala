/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.session.javadsl

import java.util.{ List => JList }
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.persistence.cassandra.SessionProvider
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.stream.javadsl.Source
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement

/**
 * Data Access Object for Cassandra. The statements are expressed in
 * <a href="http://docs.datastax.com/en/cql/3.3/cql/cqlIntro.html">Cassandra Query Language</a>
 * (CQL) syntax.
 *
 * The `init` hook is called before the underlying session is used by other methods,
 * so it can be used for things like creating the keyspace and tables.
 *
 * All methods are non-blocking.
 */
final class CassandraSession(delegate: akka.persistence.cassandra.session.scaladsl.CassandraSession) {

  /**
   * Use this constructor if you want to create a stand-alone `CassandraSession`.
   */
  def this(
    system:           ActorSystem,
    sessionProvider:  SessionProvider,
    settings:         CassandraSessionSettings,
    executionContext: ExecutionContext,
    log:              LoggingAdapter,
    metricsCategory:  String,
    init:             JFunction[Session, CompletionStage[Done]]) =
    this(new akka.persistence.cassandra.session.scaladsl.CassandraSession(
      system, sessionProvider, settings, executionContext, log, metricsCategory,
      (session => init.apply(session).toScala)))

  implicit private val ec = delegate.ec

  /**
   * The `Session` of the underlying
   * <a href="http://datastax.github.io/java-driver/">Datastax Java Driver</a>.
   * Can be used in case you need to do something that is not provided by the
   * API exposed by this class. Be careful to not use blocking calls.
   */
  def underlying(): CompletionStage[Session] =
    delegate.underlying().toJava

  /**
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateTableTOC.html">Creating a table</a>.
   *
   * The returned `CompletionStage` is completed when the table has been created,
   * or if the statement fails.
   */
  def executeCreateTable(stmt: String): CompletionStage[Done] =
    delegate.executeCreateTable(stmt).toJava

  /**
   * Create a `PreparedStatement` that can be bound and used in
   * `executeWrite` or `select` multiple times.
   */
  def prepare(stmt: String): CompletionStage[PreparedStatement] =
    delegate.prepare(stmt).toJava

  /**
   * Execute several statements in a batch. First you must [[#prepare]] the
   * statements and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatchTOC.html">Batching data insertion and updates</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `BatchStatement`.
   *
   * The returned `CompletionStage` is completed when the batch has been
   * successfully executed, or if it fails.
   */
  def executeWriteBatch(batch: BatchStatement): CompletionStage[Done] =
    delegate.executeWriteBatch(batch).toJava

  /**
   * Execute one statement. First you must [[#prepare]] the
   * statement and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  def executeWrite(stmt: Statement): CompletionStage[Done] =
    delegate.executeWrite(stmt).toJava

  /**
   * Prepare, bind and execute one statement in one go.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used.
   *
   * The returned `CompletionStage` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  @varargs
  def executeWrite(stmt: String, bindValues: AnyRef*): CompletionStage[Done] =
    delegate.executeWrite(stmt, bindValues: _*).toJava

  /**
   * Execute a select statement. First you must [[#prepare]] the
   * statement and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: Statement): Source[Row, NotUsed] =
    delegate.select(stmt).asJava

  /**
   * Prepare, bind and execute a select statement in one go.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
   *
   * The configured read consistency level is used.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  @varargs
  def select(stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] =
    delegate.select(stmt, bindValues: _*).asJava

  /**
   * Execute a select statement. First you must [[#prepare]] the statement and
   * bind its parameters. Only use this method when you know that the result
   * is small, e.g. includes a `LIMIT` clause. Otherwise you should use the
   * `select` method that returns a `Source`.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed with the found rows.
   */
  def selectAll(stmt: Statement): CompletionStage[JList[Row]] =
    delegate.selectAll(stmt).map(_.asJava).toJava

  /**
   * Prepare, bind and execute a select statement in one go. Only use this method
   * when you know that the result is small, e.g. includes a `LIMIT` clause.
   * Otherwise you should use the `select` method that returns a `Source`.
   *
   * The configured read consistency level is used.
   *
   * The returned `CompletionStage` is completed with the found rows.
   */
  @varargs
  def selectAll(stmt: String, bindValues: AnyRef*): CompletionStage[JList[Row]] =
    delegate.selectAll(stmt, bindValues: _*).map(_.asJava).toJava

  /**
   * Execute a select statement that returns one row. First you must [[#prepare]] the
   * statement and bind its parameters.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `CompletionStage` is completed with the first row,
   * if any.
   */
  def selectOne(stmt: Statement): CompletionStage[Optional[Row]] =
    delegate.selectOne(stmt).map(_.asJava).toJava

  /**
   * Prepare, bind and execute a select statement that returns one row.
   *
   * The configured read consistency level is used.
   *
   * The returned `CompletionStage` is completed with the first row,
   * if any.
   */
  @varargs
  def selectOne(stmt: String, bindValues: AnyRef*): CompletionStage[Optional[Row]] =
    delegate.selectOne(stmt, bindValues: _*).map(_.asJava).toJava

}
