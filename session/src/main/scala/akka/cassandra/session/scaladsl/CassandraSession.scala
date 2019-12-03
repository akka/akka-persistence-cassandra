/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session.scaladsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.function.{ Function => JFunction }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal
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
import akka.cassandra.session.{ CassandraSessionSettings, SessionProvider }
import akka.cassandra.session._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet

import scala.compat.java8.FutureConverters._

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
final class CassandraSession(
    system: ActorSystem,
    sessionProvider: SessionProvider,
    settings: CassandraSessionSettings,
    executionContext: ExecutionContext,
    log: LoggingAdapter,
    metricsCategory: String,
    init: CqlSession => Future[Done])
    extends NoSerializationVerificationNeeded {
  import settings._

  implicit private[akka] val ec = executionContext
  private lazy implicit val materializer = ActorMaterializer()(system)

  // cache of PreparedStatement[_](PreparedStatement[_]should only be prepared once)
  // FIXME, this can be removed as it is now built into the driver
  private val preparedStatements =
    new ConcurrentHashMap[String, Future[PreparedStatement]]
  private val computePreparedStatement =
    new JFunction[String, Future[PreparedStatement]] {
      override def apply(key: String): Future[PreparedStatement] =
        underlying().flatMap { s =>
          val prepared = s.prepareAsync(key).toScala
          prepared.failed.foreach(
            _ =>
              // this is async, i.e. we are not updating the map from the compute function
              preparedStatements.remove(key))
          prepared
        }
    }

  private val _underlyingSession = new AtomicReference[Future[CqlSession]]()

  /**
   * The `Session` of the underlying
   * <a href="http://datastax.github.io/java-driver/">Datastax Java Driver</a>.
   * Can be used in case you need to do something that is not provided by the
   * API exposed by this class. Be careful to not use blocking calls.
   */
  def underlying(): Future[CqlSession] = {

    def initialize(session: Future[CqlSession]): Future[CqlSession] =
      session.flatMap { s =>
        val result = init(s)
        result.failed.foreach(_ => close(s))
        result.map(_ => s)
      }

    @tailrec def setup(): Future[CqlSession] = {
      val existing = _underlyingSession.get
      if (existing == null) {
        val s = initialize(sessionProvider.connect())
        if (_underlyingSession.compareAndSet(null, s)) {
          s.foreach { ses =>
            try {
//              if (!ses.getCluster.isClosed())
//                CassandraMetricsRegistry(system).addMetrics(metricsCategory, ses.getCluster.getMetrics.getRegistry)
              // FIXME, how do metrics work in the new driver?
            } catch {
              case NonFatal(e) =>
                log.debug("Couldn't register metrics {}, due to {}", metricsCategory, e.getMessage)
            }

          }
          s.failed.foreach(_ => _underlyingSession.compareAndSet(s, null))
          system.registerOnTermination {
            s.foreach(close)
          }
          s
        } else {
          s.foreach(close)
          setup() // recursive
        }
      } else {
        existing
      }
    }

    val existing = _underlyingSession.get
    if (existing == null) {
      val result = retry(() => setup())
      result.failed.foreach { e =>
        log.warning(
          "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
          e.getMessage)
      }
      result
    } else
      existing
  }

  private def retry(setup: () => Future[CqlSession]): Future[CqlSession] = {
    val promise = Promise[CqlSession]

    def tryAgain(count: Int, cause: Throwable): Unit =
      if (count == 0)
        promise.failure(cause)
      else {
        system.scheduler.scheduleOnce(settings.connectionRetryDelay) {
          trySetup(count)
        }
      }

    def trySetup(count: Int): Unit =
      try {
        setup().onComplete {
          case Success(session) => promise.success(session)
          case Failure(cause)   => tryAgain(count - 1, cause)
        }
      } catch {
        case NonFatal(e) =>
          // this is only in case the direct calls, such as sessionProvider, throws
          promise.failure(e)
      }

    trySetup(settings.connectionRetries)

    promise.future
  }

  private def close(s: CqlSession): Unit = {
    s.closeAsync()
    CassandraMetricsRegistry(system).removeMetrics(metricsCategory)
  }

  def close(): Unit =
    _underlyingSession.getAndSet(null) match {
      case null     =>
      case existing => existing.foreach(close)
    }

  /**
   * This can only be used after successful initialization,
   * otherwise throws `IllegalStateException`.
   */
  def protocolVersion: ProtocolVersion =
    underlying().value match {
      case Some(Success(s)) =>
        s.getContext.getProtocolVersion
      case _ =>
        throw new IllegalStateException("protocolVersion can only be accessed after successful init")
    }

  /**
   * Execute <a href=https://docs.datastax.com/en/archived/cql/3.3/cql/cql_reference/cqlCommandsTOC.html">CQL commands</a>
   * to manage database resources (create, replace, alter, and drop tables, indexes, user-defined types, etc).
   *
   * The returned `Future` is completed when the command is done, or if the Statement[_]fails.
   */
  def executeDDL(stmt: String): Future[Done] =
    for {
      s <- underlying()
      _ <- s.executeAsync(stmt).toScala
    } yield Done

  /**
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateTableTOC.html">Creating a table</a>.
   *
   * The returned `Future` is completed when the table has been created,
   * or if the Statement[_]fails.
   */
  @deprecated("Use executeDDL instead.", "0.100")
  def executeCreateTable(stmt: String): Future[Done] = executeDDL(stmt)

  /**
   * Create a `PreparedStatement` that can be bound and used in
   * `executeWrite` or `select` multiple times.
   */
  def prepare(stmt: String): Future[PreparedStatement] =
    underlying().flatMap { _ =>
      preparedStatements.computeIfAbsent(stmt, computePreparedStatement)
    }

  /**
   * Execute several statements in a batch. First you must [[#prepare]] the
   * statements and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatchTOC.html">Batching data insertion and updates</a>.
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
   * Execute one statement. First you must [[#prepare]] the
   * Statement[_]and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `Future` is completed when the Statement[_]has been
   * successfully executed, or if it fails.
   */
  def executeWrite(stmt: Statement[_]): Future[Done] = {
    // FIXME, this is now a mutable builder, fix in all cases and/or chage it to use profiles
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(writeConsistency)

    underlying().flatMap { s =>
      s.executeAsync(stmt).toScala.map(_ => Done)
    }
  }

  /**
   * Prepare, bind and execute one Statement[_]in one go.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
   *
   * The configured write consistency level is used.
   *
   * The returned `Future` is completed when the Statement[_]has been
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
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(settings.readConsistency)
    underlying().flatMap { s =>
      s.executeAsync(stmt).toScala
    }
  }

  /**
   * Execute a select statement. First you must [[#prepare]] the
   * Statement[_]and bind its parameters.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: Statement[_]): Source[Row, NotUsed] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(readConsistency)
    Source.fromGraph(new SelectSource(Future.successful(stmt)))
  }

  /**
   * Prepare, bind and execute a select Statement[_]in one go.
   *
   * See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
   *
   * The configured read consistency level is used.
   *
   * Note that you have to connect a `Sink` that consumes the messages from
   * this `Source` and then `run` the stream.
   */
  def select(stmt: String, bindValues: AnyRef*): Source[Row, NotUsed] = {
    val bound: Future[BoundStatement] = prepare(stmt).map { ps =>
      val bs =
        if (bindValues.isEmpty) ps.bind()
        else ps.bind(bindValues: _*)
      bs.setConsistencyLevel(readConsistency)
    }
    Source.fromGraph(new SelectSource(bound))
  }

  /**
   * Execute a select statement. First you must [[#prepare]] the Statement[_]and
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
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(readConsistency)
    Source
      .fromGraph(new SelectSource(Future.successful(stmt)))
      .runWith(Sink.seq)
      .map(_.toVector) // Sink.seq returns Seq, not immutable.Seq (compilation issue in Eclipse)
  }

  /**
   * Prepare, bind and execute a select Statement[_]in one go. Only use this method
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
   * Execute a select Statement[_]that returns one row. First you must [[#prepare]] the
   * Statement[_]and bind its parameters.
   *
   * The configured read consistency level is used if a specific consistency
   * level has not been set on the `Statement`.
   *
   * The returned `Future` is completed with the first row,
   * if any.
   */
  def selectOne(stmt: Statement[_]): Future[Option[Row]] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(readConsistency)

    selectResultSet(stmt).map { rs =>
      Option(rs.one()) // rs.one returns null if exhausted
    }
  }

  /**
   * Prepare, bind and execute a select Statement[_]that returns one row.
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
              if (rs.currentPage().iterator().hasNext) {
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

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraSession {
  private val serializedExecutionProgress =
    new AtomicReference[Future[Done]](FutureDone)

  def serializedExecution(recur: () => Future[Done], exec: () => Future[Done])(
      implicit ec: ExecutionContext): Future[Done] = {
    val progress = serializedExecutionProgress.get
    val p = Promise[Done]()
    progress.onComplete { _ =>
      val result =
        if (serializedExecutionProgress.compareAndSet(progress, p.future))
          exec()
        else
          recur()
      p.completeWith(result)
      result
    }
    p.future
  }

}
