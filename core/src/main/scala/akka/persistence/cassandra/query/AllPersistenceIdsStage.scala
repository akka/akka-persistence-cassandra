/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.github.ghik.silencer.silent

import scala.collection.immutable.Queue
import scala.concurrent.duration._

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
    preparedStatement: PreparedStatement,
    session: CqlSession,
    readProfile: String)
    extends GraphStage[SourceShape[String]] {

  import AllPersistenceIdsStage._

  val out: Outlet[String] = Outlet("AllPersistenceIds.out")

  val shape: SourceShape[String] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {

      private var queryInProgress = false
      private var knownPersistenceIds: Set[String] = Set.empty
      private var maybeResultSet = Option.empty[AsyncResultSet]
      private var buffer: Queue[String] = Queue.empty[String]

      private val queryCallback: AsyncCallback[AsyncResultSet] =
        getAsyncCallback[AsyncResultSet] { rs =>
          queryInProgress = false
          maybeResultSet = Some(rs)
          rs.currentPage().forEach { row =>
            val s = row.getString("persistence_id")
            if (!knownPersistenceIds.contains(s)) {
              buffer = buffer.enqueue(s)
              knownPersistenceIds += s
            }
          }
          flush()
          if (refreshInterval.isEmpty && buffer.isEmpty && rs.hasMorePages) {
            complete(out)
          } else if (rs.hasMorePages) {
            // don't check getAvailableWithoutFetching here as it may be > 0 as they can be fetched in the background
            rs.fetchNextPage().thenAccept(queryCallback.invoke)
          }
        }

      private def isExhausted(rs: AsyncResultSet): Boolean = {
        rs.remaining() == 0 && !rs.hasMorePages
      }

      private def query(): Unit = {
        def doQuery(): Unit = {
          queryInProgress = true
          val boundStatement = preparedStatement.bind().setExecutionProfileName(readProfile)
          session.executeAsync(boundStatement).thenAccept(queryCallback.invoke)
        }
        maybeResultSet match {
          case None =>
            doQuery()
          case Some(rs) if isExhausted(rs) && !queryInProgress =>
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
              if (refreshInterval.isEmpty && isExhausted(rs)) {
                complete(out)
              } else {
                if (!queryInProgress && rs.remaining() == 0 && rs.hasMorePages) {
                  rs.fetchNextPage().thenAccept(queryCallback.invoke)
                }
              }
          }
        }
      }

      setHandler(out, this)
    }

}
