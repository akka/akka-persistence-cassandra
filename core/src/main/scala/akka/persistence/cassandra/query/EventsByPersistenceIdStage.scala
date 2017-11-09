/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.ListenableFutureConverter
import akka.persistence.cassandra.journal.CassandraJournal
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging
import akka.stream.stage.TimerGraphStageLogic
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.utils.Bytes

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
    selectDeletedToQuery:             PreparedStatement,
    session:                          Session,
    customConsistencyLevel:           Option[ConsistencyLevel],
    customRetryPolicy:                Option[RetryPolicy]
  ) {

    def selectEventsByPersistenceId(
      persistenceId: String,
      partitionNr:   Long,
      progress:      Long,
      toSeqNr:       Long,
      fetchSize:     Int
    )(implicit ec: ExecutionContext): Future[ResultSet] = {
      val boundStatement = selectEventsByPersistenceIdQuery.bind(persistenceId, partitionNr: JLong, progress: JLong, toSeqNr: JLong)
      boundStatement.setFetchSize(fetchSize)
      executeStatement(boundStatement)
    }

    def highestDeletedSequenceNumber(persistenceId: String)(implicit ec: ExecutionContext): Future[Long] = {
      executeStatement(selectDeletedToQuery.bind(persistenceId))
        .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
    }

    private def executeStatement(statement: Statement)(implicit ec: ExecutionContext): Future[ResultSet] =
      session.executeAsync(withCustom(statement)).asScala

    private def withCustom(statement: Statement): Statement = {
      customConsistencyLevel.foreach(statement.setConsistencyLevel)
      customRetryPolicy.foreach(statement.setRetryPolicy)
      statement
    }
  }

  private case object Continue
  private case object LookForMissingSeqNr

  private case class MissingSeqNr(deadline: Deadline, sawSeqNr: Long)
  private case class FastForward(seqNr: Long, partition: Int)

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(switchPartition: Boolean, fetchMore: Boolean, startTime: Long) extends QueryState
  private case class QueryResult(resultSet: ResultSet, empty: Boolean, switchPartition: Boolean) extends QueryState {
    override def toString(): String = s"QueryResult($switchPartition)"
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventsByPersistenceIdStage(persistenceId: String, fromSeqNr: Long, toSeqNr: Long, max: Long,
                                                            fetchSize: Int, refreshInterval: Option[FiniteDuration], session: EventsByPersistenceIdStage.EventsByPersistenceIdSession,
                                                            config: CassandraReadJournalConfig)
  extends GraphStageWithMaterializedValue[SourceShape[PersistentRepr], EventsByPersistenceIdStage.Control] {
  import EventsByPersistenceIdStage._

  val out: Outlet[PersistentRepr] = Outlet("EventsByPersistenceId.out")
  override val shape: SourceShape[PersistentRepr] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new TimerGraphStageLogic(shape) with OutHandler with StageLogging with Control {

      override protected def logSource: Class[_] = classOf[EventsByPersistenceIdStage]

      // lazy because materializer not initialized in constructor
      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByPersistenceId requires ActorMaterializer")
      }
      lazy val serialization = SerializationExtension(system)
      lazy val eventDeserializer = new CassandraJournal.EventDeserializer(serialization)
      implicit def ec = materializer.executionContext
      val fetchMoreThresholdRows = (fetchSize * config.fetchMoreThreshold).toInt

      val donePromise = Promise[Done]()

      var seqNr = 0L // initialized in preStart
      var partition = 0L
      var count = 0L

      var pendingPoll: Option[Long] = None
      var pendingFastForward: Option[Long] = None
      var lookingForMissingSeqNr: Option[MissingSeqNr] = None

      var queryState: QueryState = QueryIdle

      val newResultSetCb = getAsyncCallback[Try[ResultSet]] {
        case Success(rs) =>
          val q = queryState match {
            case q: QueryInProgress => q
            case _ =>
              throw new IllegalStateException(s"New ResultSet when in unexpected state $queryState")
          }
          val empty = rs.isExhausted() && !q.fetchMore
          if (log.isDebugEnabled)
            log.debug("EventsByPersistenceId [{}] Query took [{}] ms {}", persistenceId,
              (System.nanoTime() - q.startTime).nanos.toMillis,
              if (empty) "(empty)" else "")
          queryState = QueryResult(rs, empty, q.switchPartition)
          tryPushOne()
        case Failure(e) => onFailure(e)
      }

      val pollCb = getAsyncCallback[Long] { knownSeqNr =>
        if (refreshInterval.isEmpty)
          throw new IllegalStateException("External poll only possible for live queries")

        if (knownSeqNr >= seqNr) {
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

        if (nextSeqNr > seqNr) {
          queryState match {
            case QueryIdle => internalFastForward(nextSeqNr)
            case _         => pendingFastForward = Some(nextSeqNr)
          }
        }
      }

      private def internalFastForward(nextSeqNr: Long): Unit = {
        log.debug(
          "EventsByPersistenceId [{}] External fast-forward to seqNr [{}] from current [{}]",
          persistenceId, nextSeqNr, seqNr
        )
        seqNr = nextSeqNr
        val nextPartition = partitionNr(nextSeqNr)
        if (nextPartition > partition)
          partition = nextPartition
      }

      def partitionNr(sequenceNr: Long): Long =
        (sequenceNr - 1L) / config.targetPartitionSize

      override def preStart(): Unit = {
        system // fail fast if not ActorMaterializer

        queryState = QueryInProgress(switchPartition = false, fetchMore = false, System.nanoTime())
        session.highestDeletedSequenceNumber(persistenceId).onComplete {
          getAsyncCallback[Try[Long]] {
            case Success(delSeqNr) =>
              // lowest possible seqNr is 1
              seqNr = math.max(delSeqNr + 1, math.max(fromSeqNr, 1))
              partition = partitionNr(seqNr)
              // initial query
              queryState = QueryIdle
              query(switchPartition = false)

            case Failure(e) => onFailure(e)

          }.invoke
        }

        refreshInterval match {
          case Some(interval) =>
            val initial =
              if (interval >= 2.seconds)
                (interval / 2) + ThreadLocalRandom.current().nextLong(interval.toMillis / 2).millis
              else interval
            schedulePeriodicallyWithInitialDelay(Continue, initial, interval)
          case None =>
        }
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

      def continue(): Unit = {
        // regular continue-by-tick disabled when looking for missing seqNr
        if (lookingForMissingSeqNr.isEmpty) {
          queryState match {
            case QueryIdle          => query(switchPartition = false)
            case _: QueryResult     => tryPushOne()
            case _: QueryInProgress => // result will come
          }
        }
      }

      def lookForMissingSeqNr(): Unit = {
        lookingForMissingSeqNr match {
          case Some(m) if m.deadline.isOverdue() =>
            import akka.util.PrettyDuration.PrettyPrintableDuration
            onFailure(new IllegalStateException(s"Sequence number [$seqNr] still missing after " +
              s"[${config.eventsByPersistenceIdEventTimeout.pretty}], " +
              s"saw unexpected seqNr [${m.sawSeqNr}] for persistenceId [$persistenceId]."))
          case Some(_) => query(false)
          case None    => throw new IllegalStateException("Should not be able to get here")
        }
      }

      def query(switchPartition: Boolean): Unit = {
        queryState match {
          case QueryIdle          => // good
          case _: QueryInProgress => throw new IllegalStateException("Query already in progress")
          case QueryResult(rs, _, _) =>
            if (!rs.isExhausted()) throw new IllegalStateException("Previous query was not exhausted")
        }
        val pnr = if (switchPartition) partition + 1 else partition
        queryState = QueryInProgress(switchPartition, fetchMore = false, System.nanoTime())

        val endNr = lookingForMissingSeqNr match {
          case Some(_) =>
            log.debug(
              "EventsByPersistenceId [{}] Query for missing seqNr [{}] in partition [{}]",
              persistenceId, seqNr, pnr
            )
            seqNr
          case _ =>
            log.debug(
              "EventsByPersistenceId [{}] Query from seqNr [{}] in partition [{}]",
              persistenceId, seqNr, pnr
            )
            toSeqNr
        }
        session.selectEventsByPersistenceId(persistenceId, pnr, seqNr, endNr, fetchSize)
          .onComplete(newResultSetCb.invoke)
      }

      override def onPull(): Unit = {
        tryPushOne()
      }

      @tailrec private def tryPushOne(): Unit = {

        queryState match {
          case QueryResult(rs, empty, switchPartition) if isAvailable(out) =>

            def afterExhausted(): Unit = {
              queryState = QueryIdle
              // When ResultSet is exhausted we immediately look in next partition for more events.
              // We keep track of if the query was such switching partition and if result is empty
              // we complete the stage or wait until next Continue tick.
              if (empty && switchPartition && lookingForMissingSeqNr.isEmpty) {
                if (refreshInterval.isEmpty) {
                  completeStage()
                }
              } else if (pendingPoll.isDefined) {
                if (pendingPoll.get >= seqNr)
                  query(switchPartition = false)
                pendingPoll = None
              } else if (pendingFastForward.isDefined) {
                val nextNr = pendingFastForward.get
                if (nextNr > seqNr) {
                  internalFastForward(nextNr)
                  query(switchPartition = false) // FIXME should we switch here?
                }

                pendingFastForward = None
              } else {
                // TODO if we are far from the partition boundary we could skip this query if refreshInterval.nonEmpty
                query(switchPartition = true) // next partition
              }
            }

            if (reachedEndCondition())
              completeStage()
            else if (rs.isExhausted()) {
              if (lookingForMissingSeqNr.isEmpty) afterExhausted()
              else {
                queryState = QueryIdle
                scheduleOnce(LookForMissingSeqNr, 200.millis)
              }
            } else if (rs.getAvailableWithoutFetching() == 0) {
              log.debug("EventsByPersistenceId [{}] Fetch more from seqNr [{}]", persistenceId, seqNr)
              queryState = QueryInProgress(switchPartition, fetchMore = true, System.nanoTime())
              val rsFut = rs.fetchMoreResults().asScala
              rsFut.onComplete(newResultSetCb.invoke)
            } else {
              val row = rs.one()
              val event = extractEvent(row)
              if (event.sequenceNr < seqNr || pendingFastForward.isDefined && pendingFastForward.get > event.sequenceNr) {
                // skip event due to fast forward
                tryPushOne()
              } else if (pendingFastForward.isEmpty && (config.gapFreeSequenceNumbers && seqNr != event.sequenceNr)) {
                lookingForMissingSeqNr match {
                  case Some(_) => throw new IllegalStateException(
                    s"Should not be able to get here when already looking for missing seqNr [$seqNr] for entity [$persistenceId]"
                  )
                  case None =>
                    log.debug(
                      "EventsByPersistenceId [{}] Missing seqNr [{}], found [{}], looking for event eventually appear",
                      persistenceId, seqNr, event.sequenceNr
                    )
                    lookingForMissingSeqNr = Some(
                      MissingSeqNr(Deadline.now + config.eventsByPersistenceIdEventTimeout, event.sequenceNr)
                    )
                    query(false)
                }
              } else {
                // we found that missing seqNr
                if (lookingForMissingSeqNr.isDefined) {
                  log.debug("EventsByPersistenceId [{}] Found missing seqNr [{}]", persistenceId, seqNr)
                  lookingForMissingSeqNr = None
                }

                seqNr = event.sequenceNr + 1
                partition = row.getLong("partition_nr")
                count += 1
                push(out, event)

                if (reachedEndCondition())
                  completeStage()
                else if (rs.isExhausted())
                  afterExhausted()
                else if (rs.getAvailableWithoutFetching == fetchMoreThresholdRows)
                  rs.fetchMoreResults() // trigger early async fetch of more rows

              }
            }

          case QueryIdle | _: QueryInProgress | _: QueryResult => // ok

        }
      }

      def extractEvent(row: Row): PersistentRepr =
        row.getBytes("message") match {
          case null =>
            PersistentRepr(
              payload = eventDeserializer.deserializeEvent(row),
              sequenceNr = row.getLong("sequence_nr"),
              persistenceId = row.getString("persistence_id"),
              manifest = row.getString("event_manifest"), // manifest for event adapters
              deleted = false,
              sender = null,
              writerUuid = row.getString("writer_uuid")
            )
          case b =>
            // for backwards compatibility
            persistentFromByteBuffer(b)
        }

      def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
        // we know that such old rows can't have meta data because that feature was added later
        serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
      }

      def reachedEndCondition(): Boolean =
        (seqNr > toSeqNr || count >= max)

      // external call via Control materialized value
      override def poll(knownSeqNr: Long): Unit =
        try pollCb.invoke(knownSeqNr) catch {
          case _: IllegalStateException =>
          // not initialized, see Akka issue #20503, but that is ok since this
          // is just best effort
        }

      // external call via Control materialized value
      override def fastForward(nextSeqNr: Long): Unit = {
        try fastForwardCb.invoke(nextSeqNr) catch {
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
