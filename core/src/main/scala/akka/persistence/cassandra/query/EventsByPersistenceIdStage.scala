/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.ListenableFutureConverter
import akka.persistence.cassandra.journal.CassandraJournal
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler
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

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(switchPartition: Boolean, fetchMore: Boolean) extends QueryState
  private case class QueryResult(resultSet: ResultSet, empty: Boolean, switchPartition: Boolean) extends QueryState {
    override def toString(): String = s"QueryResult($switchPartition)"
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventsByPersistenceIdStage(persistenceId: String, fromSeqNr: Long, toSeqNr: Long, max: Long, fetchSize: Int,
                                                            refreshInterval: Option[FiniteDuration], session: EventsByPersistenceIdStage.EventsByPersistenceIdSession,
                                                            config: CassandraReadJournalConfig)
  extends GraphStage[SourceShape[PersistentRepr]] {
  import EventsByPersistenceIdStage._

  val out: Outlet[PersistentRepr] = Outlet("EventsByPersistenceId.out")
  override val shape: SourceShape[PersistentRepr] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {

      // lazy because materializer not initialized in constructor
      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByPersistenceId requires ActorMaterializer")
      }
      lazy val serialization = SerializationExtension(system)
      lazy val eventDeserializer = new CassandraJournal.EventDeserializer(serialization)
      implicit def ec = materializer.executionContext

      var seqNr = 0L // initialized in preStart
      var partition = 0L
      var count = 0L

      var queryState: QueryState = QueryIdle

      val newResultSetCb = getAsyncCallback[Try[ResultSet]] {
        case Success(rs) =>
          val (switchPartition, fetchMore) = queryState match {
            case QueryInProgress(switchPartition, fetchMore) => (switchPartition, fetchMore)
            case _ => throw new IllegalStateException(s"New ResultSet when in unexpected state $queryState")
          }
          val empty = rs.isExhausted() && !fetchMore
          queryState = QueryResult(rs, empty, switchPartition)
          tryPushOne()
        case Failure(e) => failStage(e)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case Continue =>
          queryState match {
            case QueryIdle          => query(switchPartition = false)
            case _: QueryResult     => tryPushOne()
            case _: QueryInProgress => // result will come
          }
      }

      override def preStart(): Unit = {
        system // fail fast if not ActorMaterializer

        def partitionNr(sequenceNr: Long): Long =
          (sequenceNr - 1L) / config.targetPartitionSize

        queryState = QueryInProgress(switchPartition = false, fetchMore = false)
        session.highestDeletedSequenceNumber(persistenceId).onComplete {
          getAsyncCallback[Try[Long]] {
            case Success(delSeqNr) =>
              // lowest possible seqNr is 1
              seqNr = math.max(delSeqNr + 1, math.max(fromSeqNr, 1))
              partition = partitionNr(seqNr)
              // initial query
              queryState = QueryIdle
              query(switchPartition = false)

            case Failure(e) => failStage(e)

          }.invoke
        }

        refreshInterval match {
          case Some(interval) =>
            val initial =
              if (interval >= 2.seconds) ThreadLocalRandom.current().nextLong(interval.toMillis).millis
              else interval
            schedulePeriodicallyWithInitialDelay(Continue, initial, interval)
          case None =>
        }
      }

      override def postStop(): Unit = {
        // for GC, in case stage is still referenced for some reason
        queryState = QueryIdle
      }

      def query(switchPartition: Boolean): Unit = {
        queryState match {
          case QueryIdle          => // good
          case _: QueryInProgress => throw new IllegalStateException("Query already in progress")
          case QueryResult(rs, _, _) =>
            if (!rs.isExhausted()) throw new IllegalStateException("Previous query was not exhausted")
        }
        val pnr = if (switchPartition) partition + 1 else partition
        queryState = QueryInProgress(switchPartition, fetchMore = false)
        session.selectEventsByPersistenceId(persistenceId, pnr, seqNr, toSeqNr, fetchSize)
          .onComplete(newResultSetCb.invoke)
      }

      override def onPull(): Unit =
        tryPushOne()

      def tryPushOne(): Unit = {

        queryState match {
          case QueryResult(rs, empty, switchPartition) if isAvailable(out) =>
            def afterExhausted(): Unit = {
              queryState = QueryIdle
              // When ResultSet is exhausted we immediately look in next partition for more events.
              // We keep track of if the query was such switching partition and if result is empty
              // we complete the stage or wait until next Continue tick.
              if (empty && switchPartition) {
                if (refreshInterval.isEmpty) {
                  completeStage()
                }
              } else
                query(switchPartition = true) // next partition
            }

            if (reachedEndCondition())
              completeStage()
            else if (rs.isExhausted())
              afterExhausted()
            else if (rs.getAvailableWithoutFetching() == 0) {
              queryState = QueryInProgress(switchPartition, fetchMore = true)
              val rsFut = rs.fetchMoreResults().asScala
              rsFut.onComplete(newResultSetCb.invoke)
            } else {
              val row = rs.one()
              val event = extractEvent(row)
              seqNr = event.sequenceNr + 1
              partition = row.getLong("partition_nr")
              count += 1
              push(out, event)

              if (reachedEndCondition())
                completeStage()
              else if (rs.isExhausted())
                afterExhausted()

            }

          case QueryIdle | _: QueryInProgress => // ok

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

      setHandler(out, this)
    }

}
