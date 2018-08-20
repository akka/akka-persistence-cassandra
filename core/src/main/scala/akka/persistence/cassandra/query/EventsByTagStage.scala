/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.{ BucketSize, TimeBucket }
import akka.persistence.cassandra.query.EventsByTagStage._
import akka.stream.stage.{ GraphStage, _ }
import akka.stream.{ ActorMaterializer, Attributes, Outlet, SourceShape }

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success, Try }
import java.lang.{ Long => JLong }

import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.CombinedEventsByTagStmts
import com.datastax.driver.core.{ ResultSet, Row, Session }
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
 *
 * For a query with that is not using an offset `usingOffset == false` then this expects
 * tag pid sequence numbers to start at 1 otherwise the first tagPidSequenceNr found is assumed
 * to be the first (scanning happens before this and results are initialTagPidSequenceNrs)
 */
@InternalApi private[akka] object EventsByTagStage {

  final case class UUIDRow(
    persistenceId:    PersistenceId,
    sequenceNr:       SequenceNr,
    offset:           UUID,
    tagPidSequenceNr: TagPidSequenceNr,
    row:              Row)

  def apply(
    session:                  TagStageSession,
    fromOffset:               UUID,
    toOffset:                 Option[UUID],
    settings:                 CassandraReadJournalConfig,
    refreshInterval:          Option[FiniteDuration],
    bucketSize:               BucketSize,
    usingOffset:              Boolean,
    initialTagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)]): EventsByTagStage = {
    new EventsByTagStage(session, fromOffset, toOffset, settings, refreshInterval, bucketSize, usingOffset, initialTagPidSequenceNrs)
  }

  private[akka] class TagStageSession(val tag: String, session: Session, statements: CombinedEventsByTagStmts, fetchSize: Int) {
    def selectEventsForBucket(bucket: TimeBucket, from: UUID, to: Option[UUID])(implicit ec: ExecutionContext): Future[ResultSet] = {
      val bound = to match {
        case Some(toUUID) =>
          statements.byTagWithUpperLimit.bind(
            tag,
            bucket.key: JLong,
            from,
            toUUID).setFetchSize(fetchSize)
        case None =>
          statements.byTag.bind(
            tag,
            bucket.key: JLong,
            from).setFetchSize(fetchSize)
      }
      session.executeAsync(bound).asScala
    }
  }

  private[akka] object TagStageSession {
    def apply(tag: String, session: Session, statements: CombinedEventsByTagStmts, fetchSize: Int): TagStageSession =
      new TagStageSession(tag, session, statements, fetchSize)
  }

  private sealed trait State
  private case object QueryIdle extends State
  private final case class QueryInProgress(startTime: Long = System.nanoTime()) extends State
  private final case class QueryResult(resultSet: ResultSet) extends State
  private final case class BufferedEvents(events: List[UUIDRow]) extends State

  private final case class LookingForMissing(
    buffered:       List[UUIDRow],
    previousOffset: UUID,
    bucket:         TimeBucket,
    queryPrevious:  Boolean,
    maxOffset:      UUID,
    persistenceId:  String,
    maxSequenceNr:  Long,
    missing:        Set[Long],
    deadline:       Deadline,
    failIfNotFound: Boolean) {

    // don't include buffered in the toString
    override def toString = s"LookingForMissing{previousOffset=$previousOffset bucket=$bucket " +
      s"queryPrevious=$queryPrevious maxOffset=$maxOffset persistenceId=$persistenceId maxSequenceNr=$maxSequenceNr " +
      s"missing=$missing deadline=$deadline failIfNotFound=$failIfNotFound"
  }

  private case object QueryPoll
}

@InternalApi private[akka] class EventsByTagStage(
  session:                  TagStageSession,
  fromOffset:               UUID,
  toOffset:                 Option[UUID],
  settings:                 CassandraReadJournalConfig,
  refreshInterval:          Option[FiniteDuration],
  bucketSize:               BucketSize,
  usingOffset:              Boolean,
  initialTagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)]) extends GraphStage[SourceShape[UUIDRow]] {

  private val out: Outlet[UUIDRow] = Outlet("event.out")

  override def shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {
      override protected def logSource = classOf[EventsByTagStage]

      var currentOffset: UUID = fromOffset
      var currTimeBucket: TimeBucket = TimeBucket(fromOffset, bucketSize)
      var state: State = QueryIdle
      var tagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)] = initialTagPidSequenceNrs
      var missingLookup: Option[LookingForMissing] = None

      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByTagStage requires ActorMaterializer")
      }

      implicit def ec: ExecutionContextExecutor = materializer.executionContext
      setHandler(out, this)

      val newResultSetCb = getAsyncCallback[Try[ResultSet]] {
        case Success(rs) =>
          val _ = state match {
            case q: QueryInProgress => q
            case _ =>
              throw new IllegalStateException(s"New ResultSet when in unexpected state $state")
          }
          state = QueryResult(rs)
          tryPushOne()
        case Failure(e) =>
          log.warning("Cassandra query failed", e)
          fail(out, e)
      }

      override def preStart(): Unit = {
        if (log.isDebugEnabled)
          log.debug(
            "Starting with initial pid tag sequence numbers: {}. Offset: {}. FromOffset: {}. To offset: {}",
            tagPidSequenceNrs, usingOffset, formatOffset(fromOffset), toOffset.map(formatOffset))

        if (settings.pubsubNotification) {
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
        }
        query()

        refreshInterval match {
          case Some(interval) =>
            val initial =
              if (interval >= 2.seconds)
                (interval / 2) + ThreadLocalRandom.current().nextLong(interval.toMillis / 2).millis
              else interval
            schedulePeriodicallyWithInitialDelay(QueryPoll, initial, interval)
            log.debug("Scheduling query poll at: {} ms", interval.toMillis)
          case None =>
            log.debug("CurrentQuery: No query polling")
        }
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case QueryPoll =>
          continue()
      }

      override def onPull(): Unit = {
        tryPushOne()
      }

      private def continue(): Unit = {
        state match {
          case QueryIdle =>
            missingLookup match {
              case None =>
                query()
              case Some(_) =>
                lookForMissing()
            }
          case _: QueryResult | _: BufferedEvents =>
            tryPushOne()
          case _: QueryInProgress =>
        }
      }

      private def query(): Unit = {
        if (log.isDebugEnabled) {
          log.debug("Executing query: timeBucket: {} offset: {}", currTimeBucket, formatOffset(currentOffset))
        }
        state = QueryInProgress()
        session.selectEventsForBucket(currTimeBucket, currentOffset, toOffset).onComplete(newResultSetCb.invoke)
      }

      private def lookForMissing(): Unit = {
        val missing = missingLookup match {
          case Some(m) => m
          case None => throw new IllegalStateException(s"lookingForMissingCalled for tag ${session.tag} when there " +
            s"is no missing. Raise a bug with debug logging.")
        }

        if (missing.deadline.isOverdue()) {
          log.info("{}: Failed to find missing sequence nr: {}", session.tag, missingLookup)
          if (missing.failIfNotFound) {
            fail(out, new IllegalStateException(s"Unable to find missing tagged event: PersistenceId: ${missing.persistenceId}. " +
              s"Tag: ${session.tag}. TagPidSequenceNr: ${missing.missing}. Previous offset: ${missing.previousOffset}"))
          } else {
            stopLookingForMissing(missing, missing.buffered)
            tryPushOne()
          }
        } else {
          state = QueryInProgress()
          if (log.isInfoEnabled) {
            log.info(
              "{}: Executing query to look for missing. Timebucket: {}. From: {}. To: {}",
              session.tag, missing.bucket, formatOffset(missing.previousOffset), formatOffset(missing.maxOffset))
          }
          session.selectEventsForBucket(missing.bucket, missing.previousOffset, Some(missing.maxOffset))
            .onComplete(newResultSetCb.invoke)
        }
      }

      def tryPushMissing(rs: ResultSet, m: LookingForMissing): Unit = {
        val row = rs.one()
        // we only extract the event if it is the missing one we've looking for
        val rowPersistenceId = row.getString("persistence_id")
        val rowTagPidSequenceNr = row.getLong("tag_pid_sequence_nr")
        if (rowPersistenceId == m.persistenceId && m.missing.contains(rowTagPidSequenceNr)) {
          val uuidRow = extractUuidRow(row)
          val remainingEvents = m.missing - uuidRow.tagPidSequenceNr
          log.info(
            "{}: Found a missing event, sequence nr {}. Remaining missing: {}",
            session.tag, uuidRow.tagPidSequenceNr, remainingEvents)
          if (remainingEvents.isEmpty) {
            stopLookingForMissing(m, uuidRow :: m.buffered)
          } else {
            log.info("{}: There are more missing events. {}", session.tag, remainingEvents)
            missingLookup = missingLookup.map(m => m.copy(
              missing = remainingEvents,
              buffered = uuidRow :: m.buffered))
          }
        }
      }

      /**
       * Works out if this is an acceptable [[TagPidSequenceNr]].
       * For a NoOffset query this must be 1.
       * For an Offset query this can be anything if we're in a bucket in the past.
       * For an Offset query for the current bucket if it is not 1 then assume that
       * all the [[TagPidSequenceNr]]s from 1 have been missed and search for them.
       * This is because before starting a [[akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.eventsByTag()]]
       * a scanning of the current bucket is done to find out the smallest [[TagPidSequenceNr]]
       */
      def handleFirstTimePersistenceId(repr: UUIDRow): Boolean = {
        val expectedSequenceNr = 1L
        if (repr.tagPidSequenceNr == expectedSequenceNr) {
          currentOffset = repr.offset
          tagPidSequenceNrs += (repr.persistenceId -> ((1, repr.offset)))
          push(out, repr)
          false
        } else if (usingOffset && (currTimeBucket.inPast || settings.eventsByTagNewPersistenceIdScanTimeout == Duration.Zero)) {
          // If we're in the past and this is an offset query we assume this is
          // the first tagPidSequenceNr
          log.debug("New persistence id: {}. Timebucket: {}. Tag pid sequence nr: {}", repr.persistenceId, currTimeBucket, repr.tagPidSequenceNr)
          currentOffset = repr.offset
          tagPidSequenceNrs += (repr.persistenceId -> ((repr.tagPidSequenceNr, repr.offset)))
          push(out, repr)
          false
        } else {
          log.info(
            "{}: Missing event for new persistence id: {}. Expected sequence nr: {}, actual: {}.",
            session.tag, repr.persistenceId, expectedSequenceNr, repr.tagPidSequenceNr)
          val previousBucketStart = UUIDs.startOf(currTimeBucket.previous(1).key)
          val startingOffset: UUID = if (UUIDComparator.comparator.compare(previousBucketStart, fromOffset) < 0) {
            log.debug("Starting at fromOffset")
            fromOffset
          } else {
            log.debug("Starting at startOfBucket")
            previousBucketStart
          }
          val bucket = TimeBucket(startingOffset, bucketSize)
          val lookInPrevious = !bucket.within(startingOffset)
          missingLookup = Some(LookingForMissing(
            repr :: Nil,
            startingOffset,
            bucket,
            lookInPrevious,
            repr.offset,
            repr.persistenceId,
            repr.tagPidSequenceNr,
            (1L until repr.tagPidSequenceNr).toSet,
            failIfNotFound = false,
            deadline = Deadline.now + settings.eventsByTagNewPersistenceIdScanTimeout))
          true
        }
      }

      def handleExistingPersistenceId(repr: UUIDRow, lastSequenceNr: Long, lastUUID: UUID): Boolean = {
        val expectedSequenceNr = lastSequenceNr + 1
        val pid = repr.persistenceId
        if (repr.tagPidSequenceNr < expectedSequenceNr) {
          log.warning("Duplicate sequence number. Persistence id: {}. Tag: {}. Expected sequence nr: {}. " +
            "Actual {}. This will be dropped.", pid, session.tag, expectedSequenceNr, repr.tagPidSequenceNr)
          false
        } else if (repr.tagPidSequenceNr > expectedSequenceNr) {
          log.info(
            "{}: Missing event for persistence id: {}. Expected sequence nr: {}, actual: {}.",
            session.tag, pid, expectedSequenceNr, repr.tagPidSequenceNr)
          val bucket = TimeBucket(repr.offset, bucketSize)
          val lookInPrevious = !bucket.within(lastUUID)
          missingLookup = Some(LookingForMissing(
            repr :: Nil,
            lastUUID,
            bucket,
            lookInPrevious,
            repr.offset,
            repr.persistenceId,
            repr.tagPidSequenceNr,
            (expectedSequenceNr until repr.tagPidSequenceNr).toSet,
            failIfNotFound = true,
            deadline = Deadline.now + settings.eventsByTagGapTimeout))
          true
        } else {
          // FIXME remove this log once stable
          if (log.isDebugEnabled)
            log.debug("Updating offset to {} from pId {} seqNr {} tagPidSequenceNr {}", formatOffset(currentOffset),
              pid, repr.sequenceNr, repr.tagPidSequenceNr)
          currentOffset = repr.offset
          tagPidSequenceNrs += (repr.persistenceId -> ((expectedSequenceNr, repr.offset)))
          push(out, repr)
          false
        }
      }

      @tailrec def tryPushOne(): Unit = {
        state match {
          case QueryResult(rs) if isAvailable(out) =>
            if (rs.isExhausted) {
              queryExhausted()
            } else if (rs.getAvailableWithoutFetching == 0) {
              log.debug("Fetching more")
              fetchMore(rs)
            } else if (missingLookup.isDefined) {
              tryPushMissing(rs, missingLookup.get)
              tryPushOne()
            } else {
              val row = rs.one()
              val repr = extractUuidRow(row)
              val pid = repr.persistenceId

              val missing = tagPidSequenceNrs.get(pid) match {
                case None =>
                  handleFirstTimePersistenceId(repr)
                case Some((lastSequenceNr, lastUUID)) =>
                  handleExistingPersistenceId(repr, lastSequenceNr, lastUUID)
              }

              if (missing) {
                lookForMissing()
              } else {
                tryPushOne()
              }
            }
          case QueryResult(rs) =>
            if (rs.isExhausted) {
              queryExhausted()
            } else if (rs.getAvailableWithoutFetching == 0) {
              log.debug("Fully fetched, getting more for next pull (not implemented yet)")
            }
          case BufferedEvents(events) if isAvailable(out) =>
            log.debug("Pushing buffered event")
            events match {
              case Nil =>
                continue()
              case e :: Nil =>
                push(out, e)
                state = QueryIdle
                query()
              case e :: tail =>
                push(out, e)
                state = BufferedEvents(tail)
                tryPushOne()
            }

          case _ =>
            log.debug("Trying to push one but no query results currently {} or demand {}", state, isAvailable(out))
        }
      }

      private def queryExhausted(): Unit = {
        log.debug("Query exhausted, new time uuid {}", currentOffset)
        if (missingLookup.isDefined) {
          val missing = missingLookup.get
          if (missing.queryPrevious) {
            log.info("{}: Missing could be in previous bucket. Querying right away.", session.tag)
            missingLookup = missingLookup.map(m => m.copy(
              queryPrevious = false,
              bucket = m.bucket.previous(1)))
            lookForMissing()
          } else {
            log.info("{}: Still looking for missing. {}. Waiting for next poll.", session.tag, missingLookup)
            state = QueryIdle
            missingLookup = missingLookup.map(m => {
              val newBucket = TimeBucket(m.maxOffset, bucketSize)
              m.copy(
                bucket = newBucket,
                queryPrevious = !newBucket.within(m.previousOffset))
            })
            // a current query doesn't have a poll we schedule
            if (refreshInterval.isEmpty)
              scheduleOnce(QueryPoll, settings.refreshInterval)
          }
        } else if (currTimeBucket.inPast) {
          nextTimeBucket()
          log.debug("Moving to next bucket: {}", currTimeBucket)
          query()
        } else {
          if (toOffset.isDefined) {
            log.debug("At today, ending.")
            completeStage()
          } else {
            state = QueryIdle
            log.debug("Nothing todo, waiting for next poll")
          }
        }
      }

      private def stopLookingForMissing(m: LookingForMissing, buffered: List[UUIDRow]): Unit = {
        state = if (buffered.isEmpty) {
          QueryIdle
        } else {
          BufferedEvents(buffered.sortBy(_.tagPidSequenceNr))
        }
        log.info("No more missing events. Sending buffered events. {}", state)
        currentOffset = m.maxOffset
        currTimeBucket = TimeBucket(m.maxOffset, bucketSize)
        tagPidSequenceNrs += (m.persistenceId -> ((m.maxSequenceNr, m.maxOffset)))
        missingLookup = None
      }

      private def fetchMore(rs: ResultSet): Unit = {
        log.debug("No more results without paging. Requesting more.")
        val moreResults: Future[ResultSet] = rs.fetchMoreResults().asScala
        state = QueryInProgress()
        moreResults.onComplete(newResultSetCb.invoke)
      }

      private def extractUuidRow(row: Row): UUIDRow = {
        UUIDRow(
          persistenceId = row.getString("persistence_id"),
          sequenceNr = row.getLong("sequence_nr"),
          offset = row.getUUID("timestamp"),
          tagPidSequenceNr = row.getLong("tag_pid_sequence_nr"),
          row)
      }

      private def nextTimeBucket(): Unit =
        currTimeBucket = currTimeBucket.next()

    }
}
