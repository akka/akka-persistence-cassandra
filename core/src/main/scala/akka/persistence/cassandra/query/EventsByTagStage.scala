/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.{ BucketSize, CassandraJournal, TimeBucket }
import akka.persistence.cassandra.query.EventsByTagStage._
import akka.serialization.SerializationExtension
import akka.stream.stage._
import akka.stream.{ ActorMaterializer, Attributes, Outlet, SourceShape }
import com.datastax.driver.core._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success, Try }
import java.lang.{ Long => JLong }

import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.persistence.cassandra.query.QueryActorPublisher.Continue
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.CombinedEventsByTagStmts
import com.datastax.driver.core.utils.UUIDs

/**
 * Walks the tag_views table.
 *
 * For current queries:
 * Moves to the next timebucket when there are no more events. If
 * the current time bucket is today then the query ends when it is exhausted.
 *
 * For live queries:
 * Plays all events up to this current time bucket. Scheduling a timer to poll for more.
 */
@InternalApi object EventsByTagStage {

  def apply(
    session:         TagStageSession,
    fromOffset:      UUID,
    toOffset:        Option[UUID],
    settings:        CassandraReadJournalConfig,
    refreshInterval: Option[FiniteDuration],
    bucketSize:      BucketSize
  ): EventsByTagStage = {
    new EventsByTagStage(session, fromOffset, toOffset, settings, refreshInterval, bucketSize)
  }

  private[akka] class TagStageSession(val tag: String, session: Session, statements: CombinedEventsByTagStmts, fetchSize: Int) {
    def selectEventsForBucket(bucket: TimeBucket, from: UUID, to: Option[UUID])(implicit ec: ExecutionContext): Future[ResultSet] = {
      val bound = to match {
        case Some(toUUID) =>
          statements.byTagWithUpperLimit.bind(
            tag,
            bucket.key: JLong,
            from,
            toUUID
          ).setFetchSize(fetchSize)
        case None =>
          statements.byTag.bind(
            tag,
            bucket.key: JLong,
            from
          ).setFetchSize(fetchSize)
      }
      session.executeAsync(bound).asScala
    }
  }

  object TagStageSession {
    def apply(tag: String, session: Session, statements: CombinedEventsByTagStmts, fetchSize: Int): TagStageSession =
      new TagStageSession(tag, session, statements, fetchSize)
  }

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(startTime: Long = System.nanoTime()) extends QueryState
  private final case class QueryResult(resultSet: ResultSet, empty: Boolean) extends QueryState
  private final case class BufferedEvents(events: List[UUIDPersistentRepr]) extends QueryState

  private final case class LookingForMissing(
    buffered:       List[UUIDPersistentRepr],
    previousOffset: UUID,
    maxOffset:      UUID,
    persistenceId:  String,
    maxSequenceNr:  Long,
    missing:        Set[Long],
    retryNr:        Int                      = 0,
    failIfNotFound: Boolean
  )

  private case object QueryPoll
}

@InternalApi class EventsByTagStage(
  session:         TagStageSession,
  fromOffset:      UUID,
  toOffset:        Option[UUID],
  settings:        CassandraReadJournalConfig,
  refreshInterval: Option[FiniteDuration],
  bucketSize:      BucketSize
) extends GraphStage[SourceShape[UUIDPersistentRepr]] {

  private val out: Outlet[UUIDPersistentRepr] = Outlet("event.out")

  override def shape = SourceShape(out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {
      var currentOffset: UUID = fromOffset
      var currTimeBucket: TimeBucket = TimeBucket(fromOffset, bucketSize)
      var queryState: QueryState = QueryIdle
      var rowsForCurrentQuery: Int = 0
      var tagPidSequenceNrs: Map[String, (Long, UUID)] = Map.empty[String, (Long, UUID)]
      var missingLookup: Option[LookingForMissing] = None

      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByTagStage requires ActorMaterializer")
      }

      lazy val serialization = SerializationExtension(system)
      lazy val eventDeserializer = new CassandraJournal.EventDeserializer(serialization)
      implicit def ec: ExecutionContextExecutor = materializer.executionContext
      setHandler(out, this)

      val newResultSetCb = getAsyncCallback[Try[ResultSet]] {
        case Success(rs) =>
          val _ = queryState match {
            case q: QueryInProgress => q
            case _ =>
              throw new IllegalStateException(s"New ResultSet when in unexpected state $queryState")
          }
          val empty = rs.isExhausted
          queryState = QueryResult(rs, empty)
          tryPushOne()
        case Failure(e) =>
          log.warning("Cassandra query failed", e)
          fail(out, e)
      }

      override def preStart(): Unit = {
        Try {
          getStageActor {
            case (_, publishedTag) =>
              if (publishedTag.equals(session.tag)) {
                log.debug("Received pub sub tag update for our tag, initiating query")
                continue()
              }
          }
          DistributedPubSub(system).mediator !
            DistributedPubSubMediator.Subscribe("akka.persistence.cassandra.journal.tag", this.stageActor.ref)
        }
        query()
        refreshInterval match {
          case Some(interval) =>
            val initial =
              if (interval >= 2.seconds)
                (interval / 2) + ThreadLocalRandom.current().nextLong(interval.toMillis / 2).millis
              else interval
            schedulePeriodicallyWithInitialDelay(QueryPoll, initial, interval)
            log.debug("Scheduling query poll at: {}", interval)
          case None =>
            log.debug("CurrentQuery: No query polling")
        }
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case QueryPoll =>
          log.debug("Timer {}", queryState)
          continue()
      }

      override def onPull(): Unit = {
        tryPushOne()
      }

      private def continue(): Unit = {
        queryState match {
          case QueryIdle =>
            missingLookup match {
              case None =>
                query()
              case Some(_) =>
                missingLookup = missingLookup.map(m => m.copy(retryNr = m.retryNr + 1))
                lookForMissing()
            }
          case _: QueryResult | _: BufferedEvents =>
            tryPushOne()
          case _: QueryInProgress =>
        }
      }

      private def query(): Unit = {
        rowsForCurrentQuery = 0
        log.debug("Executing query: timeBucket: {} offset: {}", currTimeBucket, currentOffset)
        queryState = QueryInProgress(System.nanoTime())
        session.selectEventsForBucket(currTimeBucket, currentOffset, toOffset).onComplete(newResultSetCb.invoke)
      }

      private def lookForMissing(): Unit = {
        //FIXME deal with this spanning two time buckets, any more and fail the stage
        val missing = missingLookup match {
          case Some(m) => m
          case None    => throw new IllegalStateException(s"lookingForMissingCalled when there is no missing. Raise a bug with debug logging.")
        }
        // TODO, make this configurable based on time
        if (missing.retryNr > 5) {
          log.info("Failed to find missing sequence nr: {}", missingLookup)
          if (missing.failIfNotFound) {
            fail(out, new IllegalStateException(s"Unable to find missing tagged event: PersistenceId: ${missing.persistenceId}. " +
              s"SequenceNr: ${missing.missing}. Previous offset: ${missing.previousOffset}"))
          } else {
            stopLookingForMissing(missing, missing.buffered)
            tryPushOne()
          }
        } else {
          queryState = QueryInProgress()
          val bucket = TimeBucket(missing.previousOffset, bucketSize)
          log.info("Executing query to look for missing. Timebucket: {}. From: {}. To: {}", bucket, missing.previousOffset, missing.maxOffset)
          session.selectEventsForBucket(bucket, missing.previousOffset, Some(missing.maxOffset))
            .onComplete(newResultSetCb.invoke)
        }
      }

      @tailrec def tryPushOne(): Unit = {
        log.debug("TryPushOne() {} {}", queryState, missingLookup)
        queryState match {
          case QueryResult(rs, _) if isAvailable(out) =>
            if (rs.isExhausted) {
              queryExhausted()
            } else if (rs.getAvailableWithoutFetching == 0) {
              log.debug("Fetching more")
              fetchMore(rs)
            } else if (missingLookup.isDefined) {
              val m = missingLookup.get
              val event = extractEvent(rs.one())
              if (event.persistentRepr.persistenceId == m.persistenceId && m.missing.contains(event.tagPidSequenceNr)) {
                val remainingEvents = m.missing - event.tagPidSequenceNr
                log.info("Found a missing event, sequence nr {}. Remaining missing: {}", event.tagPidSequenceNr, remainingEvents)
                if (remainingEvents.isEmpty) {
                  stopLookingForMissing(m, event :: m.buffered)
                } else {
                  log.info("There are more missing events. {}", remainingEvents)
                  missingLookup = missingLookup.map(m => m.copy(
                    missing = remainingEvents,
                    buffered = event :: m.buffered
                  ))
                }
              }
              tryPushOne()
            } else {
              rowsForCurrentQuery += 1
              val row = rs.one()
              val repr = extractEvent(row)
              //              log.debug("Received: {}", repr)
              val pid = repr.persistentRepr.persistenceId
              tagPidSequenceNrs.get(pid) match {
                case None =>
                  if (repr.tagPidSequenceNr != 1) {
                    val currentBucketStart = UUIDs.startOf(currTimeBucket.startTimestamp)
                    log.info(s"First time seeing persistenceId: $pid tagPidSqnr: ${repr.tagPidSequenceNr}. Will look from start of timebucket or offset it is later: " +
                      s"${currentBucketStart} to ${repr.offset}")
                    val startingOffset: UUID = if (UUIDComparator.comparator.compare(currentBucketStart, fromOffset) < 0) {
                      log.debug("Starting at fromOffset")
                      fromOffset
                    } else {
                      log.debug("Starting at startOfBucket")
                      currentBucketStart
                    }

                    missingLookup = Some(LookingForMissing(
                      List(repr),
                      startingOffset,
                      repr.offset,
                      repr.persistentRepr.persistenceId,
                      repr.tagPidSequenceNr,
                      (1L until repr.tagPidSequenceNr).toSet,
                      failIfNotFound = false
                    ))
                    lookForMissing()
                  } else {
                    currentOffset = row.getUUID("timestamp")
                    log.debug("Updating offset to {} from pId {} seqNr {}", currentOffset, pid, repr.persistentRepr.sequenceNr)
                    tagPidSequenceNrs += (repr.persistentRepr.persistenceId -> ((1, repr.offset)))
                    push(out, repr)
                    tryPushOne()
                  }
                case Some((lastSequenceNr, lastUUID)) =>
                  val expectedSequenceNr = lastSequenceNr + 1L
                  if (repr.tagPidSequenceNr > expectedSequenceNr) {
                    log.info("Missing event for persistence id: {}. Expected sequence nr: {}, instead got: {}.", pid, expectedSequenceNr, repr.tagPidSequenceNr)
                    missingLookup = Some(LookingForMissing(
                      List(repr),
                      lastUUID,
                      repr.offset,
                      repr.persistentRepr.persistenceId,
                      repr.tagPidSequenceNr,
                      (expectedSequenceNr until repr.tagPidSequenceNr).toSet,
                      failIfNotFound = true
                    ))
                    lookForMissing()
                  } else if (repr.tagPidSequenceNr < expectedSequenceNr) {
                    log.warning("Got duplicate sequence number. Persistence id: {}. Expected sequence nr: {}. Actual {}. This will be dropped.", pid, expectedSequenceNr, repr.tagPidSequenceNr)
                  } else {
                    currentOffset = row.getUUID("timestamp")
                    log.debug("Updating offset to {} from pId {} seqNr {}", currentOffset, pid, repr.persistentRepr.sequenceNr)
                    tagPidSequenceNrs += (repr.persistentRepr.persistenceId -> ((expectedSequenceNr, repr.offset)))
                    push(out, repr)
                    tryPushOne()
                  }
              }
            }

          case QueryResult(rs, _) =>
            if (rs.isExhausted) {
              queryExhausted()
            } else if (rs.getAvailableWithoutFetching == 0) {
              log.debug("Fully fetched, getting more for next pull (not implemented yet)")
            }

          case BufferedEvents(events) if isAvailable(out) =>
            log.debug("Pushing buffered event")
            events match {
              case e :: Nil =>
                push(out, e)
                queryState = QueryIdle
                query()
              case e :: tail =>
                push(out, e)
                queryState = BufferedEvents(tail)
                tryPushOne()
            }

          case _ =>
            log.debug("Trying to push one but no query results currently {}", queryState)
        }
      }

      private def queryExhausted(): Unit = {
        log.debug("Query exhausted, rows for this query: {}. New time uuid {}", rowsForCurrentQuery, currentOffset)
        if (missingLookup.isDefined) {
          log.info("Still looking for missing. {}. Will wait for next poll.", missingLookup)
          queryState = QueryIdle
          // a current query doesn't have a poll we schedule
          // TODO make this configurable
          if (refreshInterval.isEmpty)
            scheduleOnce(QueryPoll, 1.second)
        } else if (currTimeBucket.inPast) {
          nextTimeBucket()
          log.debug("Moving to next bucket: {}", currTimeBucket)
          query()
        } else {
          if (toOffset.isDefined) {
            log.debug("At today, ending.")
            completeStage()
          } else {
            queryState = QueryIdle
            log.debug("Nothing todo, waiting for next poll")
          }
        }
      }

      private def stopLookingForMissing(m: LookingForMissing, buffered: List[UUIDPersistentRepr]): Unit = {
        queryState = if (buffered.isEmpty) {
          QueryIdle
        } else {
          BufferedEvents(buffered.sortBy(_.tagPidSequenceNr))
        }
        log.info("No more missing events. Sending buffered events. {}", queryState)
        currentOffset = m.maxOffset
        currTimeBucket = TimeBucket(m.maxOffset, bucketSize)
        rowsForCurrentQuery = 0
        tagPidSequenceNrs += (m.persistenceId -> ((m.maxSequenceNr, m.maxOffset)))
        missingLookup = None

      }

      private def fetchMore(rs: ResultSet): Unit = {
        log.debug("No more results without paging. Requesting more.")
        val moreResults: Future[ResultSet] = rs.fetchMoreResults().asScala
        queryState = QueryInProgress()
        moreResults.onComplete(newResultSetCb.invoke)
      }

      private def extractEvent(row: Row): UUIDPersistentRepr = {
        val pr = UUIDPersistentRepr(
          row.getUUID("timestamp"),
          row.getLong("tag_pid_sequence_nr"),
          PersistentRepr(
            payload = eventDeserializer.deserializeEvent(row),
            sequenceNr = row.getLong("sequence_nr"),
            persistenceId = row.getString("persistence_id"),
            manifest = row.getString("event_manifest"), // manifest for event adapters
            deleted = false,
            sender = null,
            writerUuid = row.getString("writer_uuid")
          )
        )
        pr
      }

      private def nextTimeBucket(): Unit =
        currTimeBucket = currTimeBucket.next()
    }
}
