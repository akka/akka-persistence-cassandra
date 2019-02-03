/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.persistence.cassandra._
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.datastax.driver.core.{PreparedStatement, ResultSet, Session}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * INTERNAL API
  */
@InternalApi private[akka] object AllPersistenceIdsStage {

  case object Continue

}

/**
  * INTERNAL API
  */
@InternalApi private[akka] class AllPersistenceIdsStage(
  refreshInterval: Option[FiniteDuration],
  fetchSize: Int,
  preparedStatement: PreparedStatement,
  session: Session) extends GraphStage[SourceShape[String]] {

  import AllPersistenceIdsStage._

  val out: Outlet[String] = Outlet("AllPersistenceIds.out")

  val shape: SourceShape[String] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogicWithLogging(shape) {

    implicit def executionContext: ExecutionContextExecutor = materializer.executionContext

    var knownPersistenceIds: Set[String] = Set.empty
    var maybeResultSet = Option.empty[ResultSet]
    var buffer: Queue[String] = Queue.empty[String]

    private def query(): Future[ResultSet] = {
      val boundStatement = preparedStatement.bind()
      boundStatement.setFetchSize(fetchSize)
      session.executeAsync(boundStatement).asScala
    }

    private def pushWhileAvailable(): Unit = {
      while (buffer.nonEmpty && isAvailable(out)) {
        val (s, newBuffer) = buffer.dequeue
        buffer = newBuffer
        push(out, s)
      }
    }

    override def preStart(): Unit = {
      refreshInterval.foreach { interval =>
        val initial =
          if (interval >= 2.seconds) ThreadLocalRandom.current().nextLong(interval.toMillis).millis
          else interval
        schedulePeriodicallyWithInitialDelay(Continue, initial, interval)
      }
    }

    override def onTimer(timerKey: Any): Unit = {
      timerKey match {
        case Continue =>
          query().foreach(queryCallback.invoke)

        case _ =>
      }
    }

    val queryCallback: AsyncCallback[ResultSet] = getAsyncCallback[ResultSet] { rs =>
      maybeResultSet = Some(rs)
      val available = rs.getAvailableWithoutFetching
      for (_ <- 1 to available) {
        val s = rs.one().getString("persistence_id")
        if (!knownPersistenceIds.contains(s)) {
          buffer = buffer.enqueue(s)
          knownPersistenceIds += s
        }
      }
      pushWhileAvailable()
      if (refreshInterval.isEmpty && buffer.isEmpty && rs.getExecutionInfo.getPagingState == null) {
        complete(out)
      }
    }

    setHandler(out, new OutHandler {
      def onPull(): Unit = {
        if (buffer.isEmpty && isAvailable(out)) {
          maybeResultSet match {
            case None =>
              query().foreach(queryCallback.invoke)

            case Some(rs) =>
              if (rs.getExecutionInfo.getPagingState != null) {
                rs.fetchMoreResults().asScala.foreach(queryCallback.invoke)
              } else if (refreshInterval.isEmpty) {
                complete(out)
              }
          }
        }
      }
    })
  }

}
