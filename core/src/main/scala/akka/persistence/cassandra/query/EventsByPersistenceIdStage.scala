/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.util.concurrent.ThreadLocalRandom

import akka.Done
import akka.annotation.InternalApi
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.stream.stage._
import com.datastax.oss.driver.api.core.cql._
import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.util.{ Failure, Success, Try }

import com.datastax.oss.driver.api.core.CqlSession
import scala.annotation.nowarn
import scala.compat.java8.FutureConverters._

import akka.persistence.cassandra.PluginSettings

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventsByPersistenceIdStage {

  // materialized value
  trait Control {

    /**
     * Trigger a request to fetch more events.
     */
    def poll(knownSeqNr: Long): Unit

    /**
     * The events before `nextSeqNr` was delivered via another
     * channel and don't have to be retrieved from Cassandra.
     * It is best effort. There might still be events in flight with
     * lower sequence number that will be emitted downstream, so
     * the consumer have to handle de-duplication.
     */
    def fastForward(nextSeqNr: Long): Unit

    /**
     * Completed when the stage is stopped.
     */
    def done: Future[Done]
  }

  final case class EventsByPersistenceIdSession(
      selectEventsByPersistenceIdQuery: PreparedStatement,
      selectSingleRowQuery: PreparedStatement,
      selectDeletedToQuery: PreparedStatement,
      session: CqlSession,
      profile: String) {

    def selectEventsByPersistenceId(
        persistenceId: String,
        partitionNr: Long,
        progress: Long,
        toSeqNr: Long): Future[AsyncResultSet] = {
      val boundStatement =
        selectEventsByPersistenceIdQuery
          .bind(persistenceId, partitionNr: JLong, progress: JLong, toSeqNr: JLong)
          .setExecutionProfileName(profile)
      executeStatement(boundStatement)
    }

    def selectSingleRow(persistenceId: String, pnr: Long)(implicit ec: ExecutionContext): Future[Option[Row]] = {
      val boundStatement = selectSingleRowQuery.bind(persistenceId, pnr: JLong).setExecutionProfileName(profile)
      session.executeAsync(boundStatement).toScala.map(rs => Option(rs.one()))
    }

    def highestDeletedSequenceNumber(persistenceId: String)(implicit ec: ExecutionContext): Future[Long] =
      executeStatement(selectDeletedToQuery.bind(persistenceId).setExecutionProfileName(profile)).map(r =>
        Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))

    private def executeStatement(statement: Statement[_]): Future[AsyncResultSet] =
      session.executeAsync(statement).toScala

  }

  private case object Continue
  private case object LookForMissingSeqNr

  private case class MissingSeqNr(deadline: Deadline, sawSeqNr: Long)

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(switchPartition: Boolean, fetchMore: Boolean, startTime: Long)
      extends QueryState
  private final case class QueryResult(resultSet: AsyncResultSet, empty: Boolean, switchPartition: Boolean)
      extends QueryState {
    override def toString: String = s"QueryResult($switchPartition)"
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventsByPersistenceIdStage(
    persistenceId: String,
    fromSeqNr: Long,
    toSeqNr: Long,
    max: Long,
    refreshInterval: Option[FiniteDuration],
    session: EventsByPersistenceIdStage.EventsByPersistenceIdSession,
    settings: PluginSettings,
    executionContext: ExecutionContext,
    fastForwardEnabled: Boolean = false)
    extends GraphStageWithMaterializedValue[SourceShape[Row], EventsByPersistenceIdStage.Control] {

  import EventsByPersistenceIdStage._
  import settings.querySettings
  import settings.journalSettings

  val out: Outlet[Row] = Outlet("EventsByPersistenceId.out")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new TimerGraphStageLogic(shape) with OutHandler with StageLogging with Control {

      implicit def ec: ExecutionContext = executionContext

      override protected def logSource: Class[_] =
        classOf[EventsByPersistenceIdStage]

      val donePromise = Promise[Done]()

      var expectedNextSeqNr = 0L // initialized in preStart
      var partition = 0L
      var count = 0L

      var pendingPoll: Option[Long] = None
      var pendingFastForward: Option[Long] = None
      var lookingForMissingSeqNr: Option[MissingSeqNr] = None

      var queryState: QueryState = QueryIdle

      val newResultSetCb = getAsyncCallback[Try[AsyncResultSet]] {
        case Success(rs) =>
          val q = queryState match {
            case q: QueryInProgress => q
            case _ =>
              throw new IllegalStateException(s"New ResultSet when in unexpected state $queryState")
          }
          val empty = isExhausted(rs) && !q.fetchMore
          if (log.isDebugEnabled)
            log.debug(
              "EventsByPersistenceId [{}] Query took [{}] ms {}",
              persistenceId,
              (System.nanoTime() - q.startTime).nanos.toMillis,
              if (empty) "(empty)" else "")
          queryState = QueryResult(rs, empty, q.switchPartition)
          tryPushOne()
        case Failure(e) => onFailure(e)
      }

      val pollCb = getAsyncCallback[Long] { knownSeqNr =>
        if (refreshInterval.isEmpty)
          throw new IllegalStateException("External poll only possible for live queries")

        if (knownSeqNr >= expectedNextSeqNr) {
          log.debug("EventsByPersistenceId [{}] External poll, known seqNr [{}]", persistenceId, knownSeqNr)
          queryState match {
            case QueryIdle => query(switchPartition = false)
            case _: QueryResult | _: QueryInProgress =>
              pendingPoll = Some(knownSeqNr)
          }
        }
      }

      val fastForwardCb = getAsyncCallback[Long] { nextSeqNr =>
        if (refreshInterval.isEmpty)
          throw new IllegalStateException("Fast forward only possible for live queries")
        if (!fastForwardEnabled)
          throw new IllegalStateException("Fast forward has been disabled")

        log.debug(
          "Fast forward request being processed: Next Sequence Nr: {} Current Sequence Nr: {}",
          nextSeqNr,
          expectedNextSeqNr)
        if (nextSeqNr > expectedNextSeqNr) {
          queryState match {
            case QueryIdle => internalFastForward(nextSeqNr)
            case _ =>
              log.debug("Query in progress. Fast forward pending.")
              pendingFastForward = Some(nextSeqNr)
          }
        }
      }

      val highestDeletedSequenceNrCb = getAsyncCallback[Try[Long]] {
        case Success(delSeqNr) =>
          // lowest possible seqNr is 1
          expectedNextSeqNr = math.max(delSeqNr + 1, math.max(fromSeqNr, 1))
          partition = partitionNr(expectedNextSeqNr)
          // initial query
          queryState = QueryIdle
          query(switchPartition = false)

        case Failure(e) => onFailure(e)

      }

      val checkForGapsCb: AsyncCallback[(Int, Try[Option[Row]])] = getAsyncCallback {
        case (foundEmptyPartitionCount, result) =>
          result match {
            case Success(mbRow) =>
              mbRow.map(_.getLong("sequence_nr")) match {
                case None | Some(0) =>
                  // Some(0) when old schema with static used column, everything deleted in this partition
                  if (foundEmptyPartitionCount == 5)
                    completeStage()
                  else {
                    partition = partition + 1
                    checkForGaps(foundEmptyPartitionCount + 1)
                  }
                case Some(_) =>
                  if (foundEmptyPartitionCount == 0)
                    partition = partition + 1
                  query(switchPartition = false)
              }
            case Failure(_) =>
              throw new IllegalStateException("Should not be able to get here")
          }
      }

      private def internalFastForward(nextSeqNr: Long): Unit = {
        log.debug(
          "EventsByPersistenceId [{}] External fast-forward to seqNr [{}] from current [{}]",
          persistenceId,
          nextSeqNr,
          expectedNextSeqNr)
        expectedNextSeqNr = nextSeqNr
        val nextPartition = partitionNr(nextSeqNr)
        if (nextPartition > partition)
          partition = nextPartition
      }

      def partitionNr(sequenceNr: Long): Long =
        (sequenceNr - 1L) / journalSettings.targetPartitionSize

      override def preStart(): Unit = {
        queryState = QueryInProgress(switchPartition = false, fetchMore = false, System.nanoTime())
        session.highestDeletedSequenceNumber(persistenceId).onComplete(highestDeletedSequenceNrCb.invoke)

        refreshInterval match {
          case Some(interval) =>
            val initial =
              if (interval >= 2.seconds)
                (interval / 2) + ThreadLocalRandom.current().nextLong(interval.toMillis / 2).millis
              else interval

            scheduleContinue(initial, interval)
          case None =>
        }
      }

      @nowarn("msg=deprecated")
      private def scheduleContinue(initial: FiniteDuration, interval: FiniteDuration): Unit = {
        schedulePeriodicallyWithInitialDelay(Continue, initial, interval)
      }

      override def postStop(): Unit = {
        // for GC, in case stage is still referenced for some reason, e.g. the materialized value
        queryState = QueryIdle
        donePromise.trySuccess(Done)
      }

      def onFailure(e: Throwable): Unit = {
        donePromise.tryFailure(e)
        failStage(e)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case Continue            => continue()
        case LookForMissingSeqNr => lookForMissingSeqNr()
      }

      def continue(): Unit =
        // regular continue-by-tick disabled when looking for missing seqNr
        if (lookingForMissingSeqNr.isEmpty) {
          queryState match {
            case QueryIdle          => query(switchPartition = false)
            case _: QueryResult     => tryPushOne()
            case _: QueryInProgress => // result will come
          }
        }

      def lookForMissingSeqNr(): Unit =
        lookingForMissingSeqNr match {
          case Some(m) if m.deadline.isOverdue() =>
            import akka.util.PrettyDuration.PrettyPrintableDuration
            onFailure(
              new IllegalStateException(
                s"Sequence number [$expectedNextSeqNr] still missing after " +
                s"[${querySettings.eventsByPersistenceIdEventTimeout.pretty}], " +
                s"saw unexpected seqNr [${m.sawSeqNr}] for persistenceId [$persistenceId]."))
          case Some(_) =>
            queryState = QueryIdle
            query(false)
          case None =>
            throw new IllegalStateException("Should not be able to get here")
        }

      def query(switchPartition: Boolean): Unit = {
        queryState match {
          case QueryIdle => // good
          case _: QueryInProgress =>
            throw new IllegalStateException("Query already in progress")
          case QueryResult(rs, _, _) =>
            if (!isExhausted(rs))
              throw new IllegalStateException("Previous query was not exhausted")
        }
        val pnr = if (switchPartition) partition + 1 else partition
        queryState = QueryInProgress(switchPartition, fetchMore = false, System.nanoTime())

        val endNr = lookingForMissingSeqNr match {
          case Some(_) =>
            log.debug(
              "EventsByPersistenceId [{}] Query for missing seqNr [{}] in partition [{}]",
              persistenceId,
              expectedNextSeqNr,
              pnr)
            expectedNextSeqNr
          case _ =>
            log.debug(
              "EventsByPersistenceId [{}] Query from seqNr [{}] in partition [{}]",
              persistenceId,
              expectedNextSeqNr,
              pnr)
            toSeqNr
        }
        session
          .selectEventsByPersistenceId(persistenceId, pnr, expectedNextSeqNr, endNr)
          .onComplete(newResultSetCb.invoke)
      }

      override def onPull(): Unit =
        tryPushOne()

      @tailrec private def tryPushOne(): Unit = {
        queryState match {
          case QueryResult(rs, empty, switchPartition) if isAvailable(out) =>
            def afterExhausted(): Unit = {
              queryState = QueryIdle
              // When ResultSet is exhausted we immediately look in next partition for more events.
              // We keep track of if the query was such switching partition and if result is empty
              // we complete the stage or wait until next Continue tick.
              if (empty && switchPartition && lookingForMissingSeqNr.isEmpty) {
                if (expectedNextSeqNr < toSeqNr && !querySettings.gapFreeSequenceNumbers) {
                  log.warning(
                    s"Gap found! Checking if data in partition was deleted for {}, expected seq nr: {}, current partition nr: {}",
                    persistenceId,
                    expectedNextSeqNr,
                    partition)
                  checkForGaps(foundEmptyPartitionCount = 0)
                } else if (refreshInterval.isEmpty) {
                  completeStage()
                } else {
                  pendingFastForward.foreach { nextNr =>
                    if (nextNr > expectedNextSeqNr)
                      internalFastForward(nextNr)
                    pendingFastForward = None
                  }
                  pendingPoll.foreach { pollNr =>
                    if (pollNr >= expectedNextSeqNr)
                      query(switchPartition = false)
                    pendingPoll = None
                  }
                }
              } else {
                // TODO if we are far from the partition boundary we could skip this query if refreshInterval.nonEmpty
                query(switchPartition = true) // next partition
              }
            }

            if (reachedEndCondition())
              completeStage()
            else if (isExhausted(rs)) {
              (lookingForMissingSeqNr, pendingFastForward) match {
                case (Some(MissingSeqNr(_, sawSeqNr)), Some(fastForwardTo)) if fastForwardTo >= sawSeqNr =>
                  log.debug(
                    "Aborting missing sequence search: {} nr due to fast forward to next sequence nr: {}",
                    lookingForMissingSeqNr,
                    fastForwardTo)
                  internalFastForward(fastForwardTo)
                  pendingFastForward = None
                  lookingForMissingSeqNr = None
                  afterExhausted()
                case (Some(_), None) =>
                  queryState = QueryIdle
                  scheduleOnce(LookForMissingSeqNr, 200.millis)
                case _ =>
                  afterExhausted()
              }
            } else if (rs.remaining() == 0) {
              log.debug("EventsByPersistenceId [{}] Fetch more from seqNr [{}]", persistenceId, expectedNextSeqNr)
              queryState = QueryInProgress(switchPartition, fetchMore = true, System.nanoTime())
              val rsFut = rs.fetchNextPage().toScala
              rsFut.onComplete(newResultSetCb.invoke)
            } else {
              val row = rs.one()
              val sequenceNr = extractSeqNr(row)
              if ((sequenceNr < expectedNextSeqNr && fastForwardEnabled) || pendingFastForward.isDefined && pendingFastForward.get > sequenceNr) {
                // skip event due to fast forward
                tryPushOne()
              } else if (pendingFastForward.isEmpty && querySettings.gapFreeSequenceNumbers && sequenceNr > expectedNextSeqNr) {
                // we will probably now come in here which isn't what we want
                lookingForMissingSeqNr match {
                  case Some(_) =>
                    throw new IllegalStateException(
                      s"Should not be able to get here when already looking for missing seqNr [$expectedNextSeqNr] for entity [$persistenceId]")
                  case None =>
                    log.debug(
                      "EventsByPersistenceId [{}] Missing seqNr [{}], found [{}], looking for event eventually appear",
                      persistenceId,
                      expectedNextSeqNr,
                      sequenceNr)
                    lookingForMissingSeqNr = Some(
                      MissingSeqNr(Deadline.now + querySettings.eventsByPersistenceIdEventTimeout, sequenceNr))
                    // Forget about any other rows in this result set until we find
                    // the missing sequence nrs
                    queryState = QueryIdle
                    query(false)
                }
              } else {
                expectedNextSeqNr = sequenceNr + 1
                partition = row.getLong("partition_nr")
                count += 1
                push(out, row)

                if (reachedEndCondition())
                  completeStage()
                else if (lookingForMissingSeqNr.isDefined) {
                  // we found that missing seqNr
                  log.debug("EventsByPersistenceId [{}] Found missing seqNr [{}]", persistenceId, sequenceNr)
                  lookingForMissingSeqNr = None
                  queryState = QueryIdle
                  if (refreshInterval.isEmpty) query(false)
                  else afterExhausted()

                } else if (isExhausted(rs)) {
                  afterExhausted()
                }

              }
            }

          case QueryIdle | _: QueryInProgress | _: QueryResult => // ok

        }
      }

      // See PR #509 for background
      // Only used when gapFreeSequenceNumbers==false
      // if full partition was cleaned up we look for two empty partitions before completing
      def checkForGaps(foundEmptyPartitionCount: Int): Unit = {
        session
          .selectSingleRow(persistenceId, partition)
          .onComplete(result => checkForGapsCb.invoke((foundEmptyPartitionCount, result)))
      }

      def extractSeqNr(row: Row): Long = row.getLong("sequence_nr")

      def reachedEndCondition(): Boolean =
        expectedNextSeqNr > toSeqNr || count >= max

      // external call via Control materialized value
      override def poll(knownSeqNr: Long): Unit =
        try pollCb.invoke(knownSeqNr)
        catch {
          case _: IllegalStateException =>
          // not initialized, see Akka issue #20503, but that is ok since this
          // is just best effort
        }

      // external call via Control materialized value
      override def fastForward(nextSeqNr: Long): Unit = {
        log.debug("Received fast forward request {}", nextSeqNr)
        if (!fastForwardEnabled)
          throw new IllegalStateException("Fast forward only has been disabled")

        try fastForwardCb.invoke(nextSeqNr)
        catch {
          case _: IllegalStateException =>
          // not initialized, see Akka issue #20503, but that is ok since this
          // is just best effort
        }
      }

      // materialized value
      override def done: Future[Done] = donePromise.future

      setHandler(out, this)
    }
    (logic, logic)
  }
}
