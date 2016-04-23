/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.util.function.{ Function => JFunction }
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal
import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import java.util.concurrent.ConcurrentHashMap

/**
 * INTERNAL API
 */
private[cassandra] final class CassandraSession(
  system: ActorSystem, settings: CassandraPluginConfig, executionContext: ExecutionContext,
  log:             LoggingAdapter,
  metricsCategory: String,
  init:            Session => Future[_]
) {

  implicit private val ec = executionContext

  // cache of PreparedStatement (PreparedStatement should only be prepared once)
  private val preparedStatements = new ConcurrentHashMap[String, Future[PreparedStatement]]
  private val computePreparedStatement = new JFunction[String, Future[PreparedStatement]] {
    override def apply(key: String): Future[PreparedStatement] =
      underlying().flatMap { s =>
        val prepared: Future[PreparedStatement] = s.prepareAsync(key)
        prepared.onFailure {
          case _ =>
            // this is async, i.e. we are not updating the map from the compute function
            preparedStatements.remove(key)
        }
        prepared
      }
  }

  private val _underlyingSession = new AtomicReference[Future[Session]]()

  final def underlying(): Future[Session] = {

    def initialize(session: Future[Session]): Future[Session] = {
      session.flatMap { s =>
        val result = init(s)
        result.onFailure { case _ => close(s) }
        result.map(_ => s)
      }
    }

    @tailrec def setup(): Future[Session] = {
      val existing = _underlyingSession.get
      if (existing == null) {
        val s = initialize(settings.sessionProvider.connect())
        if (_underlyingSession.compareAndSet(null, s)) {
          s.foreach { ses =>
            try {
              if (!ses.getCluster.isClosed())
                CassandraMetricsRegistry(system).addMetrics(metricsCategory, ses.getCluster.getMetrics.getRegistry)
            } catch {
              case NonFatal(e) => log.debug("Couldn't register metrics {}, due to {}", metricsCategory, e.getMessage)
            }

          }
          s.onFailure {
            case e =>
              _underlyingSession.compareAndSet(s, null)
          }
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
      result.onFailure {
        case e => log.warning(
          "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
          e.getMessage
        )
      }
      result
    } else
      existing
  }

  private def retry(setup: () => Future[Session]): Future[Session] = {
    val promise = Promise[Session]

    def tryAgain(count: Int, cause: Throwable): Unit = {
      if (count == 0)
        promise.failure(cause)
      else {
        system.scheduler.scheduleOnce(settings.connectionRetryDelay) {
          trySetup(count)
        }
      }
    }

    def trySetup(count: Int): Unit = {
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
    }

    trySetup(settings.connectionRetries)

    promise.future
  }

  def prepare(stmt: String): Future[PreparedStatement] =
    underlying().flatMap { _ =>
      preparedStatements.computeIfAbsent(stmt, computePreparedStatement)
    }

  def execute(stmt: Statement): Future[ResultSet] = underlying().flatMap(_.executeAsync(stmt))

  def executeWrite(stmt: Statement): Future[Unit] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(settings.writeConsistency)
    underlying().flatMap { s =>
      s.executeAsync(stmt).map(_ => ())
    }
  }

  def select(stmt: Statement): Future[ResultSet] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(settings.readConsistency)
    underlying().flatMap { s =>
      s.executeAsync(stmt)
    }
  }

  private def close(s: Session): Unit = {
    s.closeAsync()
    s.getCluster().closeAsync()
    CassandraMetricsRegistry(system).removeMetrics(metricsCategory)
  }

  def close(): Unit = {
    _underlyingSession.getAndSet(null) match {
      case null     =>
      case existing => existing.foreach(close)
    }
  }

  /**
   * This can only be used after successful initialization,
   * otherwise throws `IllegalStateException`.
   */
  def protocolVersion: ProtocolVersion =
    underlying().value match {
      case Some(Success(s)) => s.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      case _                => throw new IllegalStateException("protocolVersion can only be accessed after successful init")
    }

}

/**
 * INTERNAL API
 */
private[cassandra] final object CassandraSession {
  private val serializedExecutionProgress = new AtomicReference[Future[Done]](Future.successful(Done))

  def serializedExecution(recur: () => Future[Done], exec: () => Future[Done])(implicit ec: ExecutionContext): Future[Done] = {
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
