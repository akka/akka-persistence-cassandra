/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.Statement
import akka.annotation.InternalApi
import akka.stream.alpakka.cassandra.{ CassandraMetricsRegistry, CqlSessionProvider }
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import scala.compat.java8.FutureConverters._

import akka.stream.alpakka.cassandra.CassandraServerMetaData
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException

/**
 * Data Access Object for Cassandra. The statements are expressed in
 * <a href="https://cassandra.apache.org/doc/latest/cql/">Apache Cassandra Query Language</a>
 * (CQL) syntax.
 *
 * See even <a href="https://docs.datastax.com/en/dse/6.7/cql/">CQL for Datastax Enterprise</a>.
 *
 * The `init` hook is called before the underlying session is used by other methods,
 * so it can be used for things like creating the keyspace and tables.
 *
 * All methods are non-blocking.
 */
final class CassandraSession(
    system: ActorSystem,
    sessionProvider: CqlSessionProvider,
    executionContext: ExecutionContext,
    log: LoggingAdapter,
    metricsCategory: String,
    init: CqlSession => Future[Done],
    onClose: () => Unit)
    extends NoSerializationVerificationNeeded {

  implicit private[akka] val ec = executionContext
  private lazy implicit val materializer = ActorMaterializer()(system)

  log.debug("Starting CassandraSession [{}]", metricsCategory)

  private var cachedServerMetaData: OptionVal[Future[CassandraServerMetaData]] = OptionVal.None

  private val _underlyingSession: Future[CqlSession] = sessionProvider.connect().flatMap { session =>
    if (log.isDebugEnabled) {
      val version = session.getContext.getProtocolVersion
      log.debug(
        "Underlying CqlSession established, using protocol version " +
        s"[$version code=${version.getCode} beta=${version.isBeta} name=${version.name()}]")
    }
    session.getMetrics.ifPresent(metrics => {
      CassandraMetricsRegistry(system).addMetrics(metricsCategory, metrics.getRegistry)
    })
    init(session).map(_ => session)
  }

  /**
   * The `Session` of the underlying
   * <a href="https://docs.datastax.com/en/developer/java-driver/">Datastax Java Driver</a>.
   * Can be used in case you need to do something that is not provided by the
   * API exposed by this class. Be careful to not use blocking calls.
   */
  def underlying(): Future[CqlSession] = _underlyingSession

  /**
   * Closes the underlying Cassandra session.
   * @param executionContext when used after actor system termination, the a different execution context must be provided
   */
  def close(executionContext: ExecutionContext): Future[Done] = {
    implicit val ec: ExecutionContext = executionContext
    onClose()
    _underlyingSession.map(_.closeAsync().toScala).map(_ => Done)
  }

  /**
   * The `ProtocolVersion` used by the driver to communicate with the server.
   */
  def protocolVersion: Future[ProtocolVersion] =
    underlying().map(_.getContext.getProtocolVersion)

  /**
   * Meta data about the Cassandra server, such as its version.
   */
  def serverMetaData: Future[CassandraServerMetaData] = {
    cachedServerMetaData match {
      case OptionVal.Some(cached) =>
        cached
      case OptionVal.None =>
        val result = selectOne("select cluster_name, data_center, release_version from system.local").map {
          case Some(row) =>
            new CassandraServerMetaData(
              row.getString("cluster_name"),
              row.getString("data_center"),
              row.getString("release_version"))
          case None =>
            log.warning("Couldn't retrieve serverMetaData from system.local table. No rows found.")
            new CassandraServerMetaData("", "", "")
        }

        result.foreach { meta =>
          cachedServerMetaData = OptionVal.Some(Future.successful(meta))
        }
        result.failed.foreach {
          case e: InvalidQueryException =>
            log.warning("Couldn't retrieve serverMetaData from system.local table: [{}]", e.getMessage)
            cachedServerMetaData = OptionVal.Some(Future.successful(new CassandraServerMetaData("", "", "")))
          case _ => // don't cache other problems, like connection errors
        }

        result
    }
  }

  /**
   * Execute <a href="https://docs.datastax.com/en/dse/6.7/cql/">CQL commands</a>
   * to manage database resources (create, replace, alter, and drop tables, indexes, user-defined types, etc).
   *
   * The returned `Future` is completed when the command is done, or if the statement fails.
   */
  def executeDDL(stmt: String): Future[Done] =
    for {
      s <- underlying()
      _ <- s.executeAsync(stmt).toScala
    } yield Done

  /**
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useCreateTable.html">Creating a table</a>.
   *
   * The returned `Future` is completed when the table has been created,
   * or if the statement fails.
   */
  @deprecated("Use executeDDL instead.", "0.100")
  def executeCreateTable(stmt: String): Future[Done] = executeDDL(stmt)

  /**
   * Create a `PreparedStatement` that can be bound and used in
   * `executeWrite` or `select` multiple times.
   */
  def prepare(stmt: String): Future[PreparedStatement] =
    underlying().flatMap { session =>
      session.prepareAsync(stmt).toScala
    }

  /**
   * Execute several statements in a batch. First you must `prepare` the
   * statements and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useBatchTOC.html">Batching data insertion and updates</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `BatchStatement`.
   *
   * The returned `Future` is completed when the batch has been
   * successfully executed, or if it fails.
   */
  def executeWriteBatch(batch: BatchStatement): Future[Done] =
    executeWrite(batch)

  /**
   * Execute one statement. First you must `prepare` the
   * statement and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `Future` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  def executeWrite(stmt: Statement[_]): Future[Done] = {
    underlying().flatMap { s =>
      s.executeAsync(stmt).toScala.map(_ => Done)
    }
  }

  /**
   * Prepare, bind and execute one statement in one go.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used.
   *
   * The returned `Future` is completed when the statement has been
   * successfully executed, or if it fails.
   */
  def executeWrite(stmt: String, bindValues: AnyRef*): Future[Done] = {
    val bound: Future[BoundStatement] = prepare(stmt).map { ps =>
      val bs =
        if (bindValues.isEmpty) ps.bind()
        else ps.bind(bindValues: _*)
      bs
    }
    bound.flatMap(b => executeWrite(b))
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def selectResultSet(stmt: Statement[_]): Future[AsyncResultSet] = {
    underlying().flatMap { s =>
      s.executeAsync(stmt).toScala
    }
  }

  /**
   * Execute a select statement. First you must `prepare` the
   * statement and bind its parameters.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: Statement[_]): Source[Row, NotUsed] = {
    Source.fromGraph(new SelectSource(Future.successful(stmt)))
  }

  /**
   * Execute a select statement created by `prepare`.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: Future[Statement[_]]): Source[Row, NotUsed] = {
    Source.fromGraph(new SelectSource(stmt))
  }

  /**
   * Prepare, bind and execute a select statement in one go.
   *
   * See <a href="https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/queriesTOC.html">Querying data</a>.
   *
   * The configured read consistency level is used.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] = {
    val bound = prepare(stmt).map { ps =>
      if (bindValues.isEmpty) ps.bind()
      else ps.bind(bindValues: _*)
    }
    Source.fromGraph(new SelectSource(bound))
  }

  /**
   * Execute a select statement. First you must `prepare` the statement and
   * bind its parameters. Only use this method when you know that the result
   * is small, e.g. includes a `LIMIT` clause. Otherwise you should use the
   * `select` method that returns a `Source`.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `Future` is completed with the found rows.
   */
  def selectAll(stmt: Statement[_]): Future[immutable.Seq[Row]] = {
    Source
      .fromGraph(new SelectSource(Future.successful(stmt)))
      .runWith(Sink.seq)
      .map(_.toVector) // Sink.seq returns Seq, not immutable.Seq (compilation issue in Eclipse)
  }

  /**
   * Prepare, bind and execute a select statement in one go. Only use this method
   * when you know that the result is small, e.g. includes a `LIMIT` clause.
   * Otherwise you should use the `select` method that returns a `Source`.
   *
   * The configured read consistency level is used.
   *
   * The returned `Future` is completed with the found rows.
   */
  def selectAll(stmt: String, bindValues: AnyRef*): Future[immutable.Seq[Row]] = {
    val bound: Future[BoundStatement] = prepare(stmt).map(
      ps =>
        if (bindValues.isEmpty) ps.bind()
        else ps.bind(bindValues: _*))
    bound.flatMap(bs => selectAll(bs))
  }

  /**
   * Execute a select statement that returns one row. First you must `prepare` the
   * statement and bind its parameters.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `Future` is completed with the first row,
   * if any.
   */
  def selectOne(stmt: Statement[_]): Future[Option[Row]] = {

    selectResultSet(stmt).map { rs =>
      Option(rs.one()) // rs.one returns null if exhausted
    }
  }

  /**
   * Prepare, bind and execute a select statement that returns one row.
   *
   * The configured read consistency level is used.
   *
   * The returned `Future` is completed with the first row,
   * if any.
   */
  def selectOne(stmt: String, bindValues: AnyRef*): Future[Option[Row]] = {
    val bound: Future[BoundStatement] = prepare(stmt).map(
      ps =>
        if (bindValues.isEmpty) ps.bind()
        else ps.bind(bindValues: _*))
    bound.flatMap(bs => selectOne(bs))
  }

  private class SelectSource(stmt: Future[Statement[_]]) extends GraphStage[SourceShape[Row]] {

    private val out: Outlet[Row] = Outlet("rows")
    override val shape: SourceShape[Row] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var asyncResult: AsyncCallback[AsyncResultSet] = _
        var asyncFailure: AsyncCallback[Throwable] = _
        var resultSet: Option[AsyncResultSet] = None

        override def preStart(): Unit = {
          asyncResult = getAsyncCallback[AsyncResultSet] { rs =>
            resultSet = Some(rs)
            tryPushOne()
          }
          asyncFailure = getAsyncCallback { e =>
            fail(out, e)
          }
          stmt.failed.foreach(e => asyncFailure.invoke(e))
          stmt.foreach { s =>
            val rsFut = underlying().flatMap(_.executeAsync(s).toScala)
            rsFut.failed.foreach { e =>
              asyncFailure.invoke(e)
            }
            rsFut.foreach(asyncResult.invoke)
          }
        }

        setHandler(out, new OutHandler {
          override def onPull(): Unit =
            tryPushOne()
        })

        def tryPushOne(): Unit =
          resultSet match {
            case Some(rs) if isAvailable(out) =>
              if (rs.remaining() > 0) {
                push(out, rs.one())
              } else if (rs.hasMorePages) {
                val next = rs.fetchNextPage()
                next.whenComplete { (result, throwable) =>
                  if (result != null) {
                    asyncResult.invoke(result)
                  } else {
                    asyncFailure.invoke(throwable)
                  }
                }
              } else {
                complete(out)
              }
            case _ =>
          }
      }
  }

}
