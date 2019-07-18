/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.cassandra.session._
import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.datastax.driver.core.{ PreparedStatement, ResultSet, Session }
import com.github.ghik.silencer.silent

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AllPersistenceIdsStage {

  case object Continue

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class AllPersistenceIdsStage(
    refreshInterval: Option[FiniteDuration],
    fetchSize: Int,
    preparedStatement: PreparedStatement,
    session: Session)
    extends GraphStage[SourceShape[String]] {

  import AllPersistenceIdsStage._

  val out: Outlet[String] = Outlet("AllPersistenceIds.out")

  val shape: SourceShape[String] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {

      implicit def executionContext: ExecutionContextExecutor = materializer.executionContext

      private var queryInProgress = false
      private var knownPersistenceIds: Set[String] = Set.empty
      private var maybeResultSet = Option.empty[ResultSet]
      private var buffer: Queue[String] = Queue.empty[String]

      private val queryCallback: AsyncCallback[ResultSet] =
        getAsyncCallback[ResultSet] { rs =>
          queryInProgress = false
          maybeResultSet = Some(rs)
          val available = rs.getAvailableWithoutFetching
          for (_ <- 1 to available) {
            val s = rs.one().getString("persistence_id")
            if (!knownPersistenceIds.contains(s)) {
              buffer = buffer.enqueue(s)
              knownPersistenceIds += s
            }
          }
          flush()
          if (refreshInterval.isEmpty && buffer.isEmpty && rs.isExhausted) {
            complete(out)
          } else if (!rs.isExhausted) {
            // don't check getAvailableWithoutFetching here as it may be > 0 as they can be fetched in the background
            rs.fetchMoreResults().asScala.foreach(queryCallback.invoke)
          }
        }

      private def query(): Unit = {
        def doQuery(): Unit = {
          queryInProgress = true
          val boundStatement = preparedStatement.bind()
          boundStatement.setFetchSize(fetchSize)
          session.executeAsync(boundStatement).asScala.foreach(queryCallback.invoke)
        }
        maybeResultSet match {
          case None =>
            doQuery()
          case Some(rs) if rs.isExhausted && !queryInProgress =>
            doQuery()
          case _ =>
          // ignore query request as either a query is in progress or there's a result set
          // which isn't fully exhausted
        }
      }

      private def flush(): Unit = {
        while (buffer.nonEmpty && isAvailable(out)) {
          val (s, newBuffer) = buffer.dequeue
          buffer = newBuffer
          push(out, s)
        }
      }

      @silent("deprecated") // keep compatible with akka 2.5
      override def preStart(): Unit = {
        query()
        refreshInterval.foreach { interval =>
          val initial =
            if (interval >= 2.seconds)
              (interval / 2) + ThreadLocalRandom.current().nextLong(interval.toMillis / 2).millis
            else interval

          schedulePeriodicallyWithInitialDelay(Continue, initial, interval)
        }
      }

      override def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case Continue =>
            query()

          case _ =>
        }
      }

      def onPull(): Unit = {
        flush()
        if (buffer.isEmpty && isAvailable(out)) {
          maybeResultSet match {
            case None =>
              query()

            case Some(rs) =>
              if (refreshInterval.isEmpty && rs.isExhausted) {
                complete(out)
              } else {
                if (!queryInProgress && !rs.isFullyFetched) {
                  rs.fetchMoreResults().asScala.foreach(queryCallback.invoke)
                }
              }
          }
        }
      }

      setHandler(out, this)
    }

}
