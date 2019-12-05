/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success, Try }

import akka.annotation.InternalApi
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.{ BucketSize, TimeBucket }
import akka.persistence.cassandra.query.EventsByTagStage._
import akka.stream.stage.{ GraphStage, _ }
import akka.stream.{ ActorMaterializer, Attributes, Outlet, SourceShape }
import akka.cassandra.session._
import akka.util.PrettyDuration._
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.EventByTagStatements
import akka.persistence.query.Offset
import com.datastax.driver.core.{ ResultSet, Row, Session }
import com.datastax.driver.core.utils.UUIDs
import com.github.ghik.silencer.silent

/**
 * INTERNAL API
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

  final class MissingTaggedEventException(
      val tag: Tag,
      val persistenceId: PersistenceId,
      missingTagPidSequenceNrs: Set[Long],
      val lastKnownOffset: Offset)
      extends RuntimeException(
        s"Unable to find missing tagged event: PersistenceId: $persistenceId. " +
        s"Tag: $tag. MissingTagPidSequenceNrs: $missingTagPidSequenceNrs. Previous offset: $lastKnownOffset")

  final case class UUIDRow(
      persistenceId: PersistenceId,
      sequenceNr: SequenceNr,
      offset: UUID,
      tagPidSequenceNr: TagPidSequenceNr,
      row: Row) {
    // don't include row
    override def toString: String =
      s"pid: $persistenceId, sequenceNr: $sequenceNr, offset: $offset, tagPidSequenceNr: $tagPidSequenceNr"
  }

  def apply(
      session: TagStageSession,
      fromOffset: UUID,
      toOffset: Option[UUID],
      settings: CassandraReadJournalConfig,
      refreshInterval: Option[FiniteDuration],
      bucketSize: BucketSize,
      usingOffset: Boolean,
      initialTagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)]): EventsByTagStage =
    new EventsByTagStage(
      session,
      fromOffset,
      toOffset,
      settings,
      refreshInterval,
      bucketSize,
      usingOffset,
      initialTagPidSequenceNrs)

  @InternalApi private[akka] class TagStageSession(
      val tag: String,
      session: Session,
      statements: EventByTagStatements,
      fetchSize: Int) {
    def selectEventsForBucket(bucket: TimeBucket, from: UUID, to: UUID)(
        implicit ec: ExecutionContext): Future[ResultSet] = {
      val bound =
        statements.byTagWithUpperLimit.bind(tag, bucket.key: JLong, from, to).setFetchSize(fetchSize)

      session.executeAsync(bound).asScala
    }
  }

  @InternalApi private[akka] object TagStageSession {
    def apply(tag: String, session: Session, statements: EventByTagStatements, fetchSize: Int): TagStageSession =
      new TagStageSession(tag, session, statements, fetchSize)
  }

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(startTime: Long = System.nanoTime()) extends QueryState
  private final case class QueryResult(resultSet: ResultSet) extends QueryState
  private final case class BufferedEvents(events: List[UUIDRow]) extends QueryState

  /**
   * @param gapDetected Whether an explicit gap has been detected e.g. events 1-4 have been seen then the next event isn not 5.
   *                    The other scenario is that the first event for a persistence id is not 1, which when starting from an offset
   *                    won't fail the stream as this is normal. However due to the eventual consistency of C* the stream still looks for earlier
   *                    events for a short time.
   */
  private final case class LookingForMissing(
      buffered: List[UUIDRow],
      previousOffset: UUID,
      bucket: TimeBucket,
      queryPrevious: Boolean,
      maxOffset: UUID,
      persistenceId: String,
      maxSequenceNr: Long,
      missing: Set[Long],
      deadline: Deadline,
      gapDetected: Boolean) {

    // don't include buffered in the toString
    override def toString =
      s"LookingForMissing{previousOffset=$previousOffset bucket=$bucket " +
      s"queryPrevious=$queryPrevious maxOffset=$maxOffset persistenceId=$persistenceId maxSequenceNr=$maxSequenceNr " +
      s"missing=$missing deadline=$deadline gapDetected=$gapDetected"
  }

  private case object QueryPoll
  private case object TagNotification

  private case class StageState(
      state: QueryState,
      // updated each time a new row is read, may skip delayed events with lower offsets
      // these gaps will be detected for the same pid, for cross pid delayed events it
      // relies on the eventual consistency delay
      fromOffset: UUID,
      // upper limit for next. Always kept at least the eventual consistency in the past
      toOffset: UUID,
      // for each persistence id what is the latest tag pid sequenceNr number and offset
      tagPidSequenceNrs: Map[PersistenceId, (TagPidSequenceNr, UUID)],
      missingLookup: Option[LookingForMissing],
      bucketSize: BucketSize) {

    val isLookingForMissing: Boolean = missingLookup.isDefined

    lazy val currentTimeBucket: TimeBucket = TimeBucket(fromOffset, bucketSize)

    /**
     * Move to the next bucket if it is in the past. Also if eventual consistency
     * delay has set a toOffset within this bucket stay in ths bucket until it is updated
     * to be after.
     */
    def shouldMoveBucket(): Boolean =
      currentTimeBucket.inPast && !currentTimeBucket.within(toOffset)

    def tagPidSequenceNumberUpdate(pid: PersistenceId, tagPidSequenceNr: (TagPidSequenceNr, UUID)): StageState =
      copy(tagPidSequenceNrs = tagPidSequenceNrs + (pid -> tagPidSequenceNr))

    // override to give nice offset formatting
    override def toString: String =
      s"""StageState(state: $state, fromOffset: ${formatOffset(fromOffset)}, toOffset: ${formatOffset(toOffset)}, tagPidSequenceNrs: $tagPidSequenceNrs, missingLookup: $missingLookup, bucketSize: $bucketSize)"""
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventsByTagStage(
    session: TagStageSession,
    fromOffset: UUID,
    toOffset: Option[UUID],
    settings: CassandraReadJournalConfig,
    refreshInterval: Option[FiniteDuration],
    bucketSize: BucketSize,
    usingOffset: Boolean,
    initialTagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)])
    extends GraphStage[SourceShape[UUIDRow]] {

  private val out: Outlet[UUIDRow] = Outlet("event.out")
  private val verboseDebug = settings.eventsByTagDebug

  override def shape = SourceShape(out)

  val stageUuid = UUID.randomUUID().toString

  private def isLiveQuery(): Boolean = refreshInterval.isDefined

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {
      override protected def logSource = classOf[EventsByTagStage]

      var stageState: StageState = _
      val toOffsetMillis =
        toOffset.map(UUIDs.unixTimestamp).getOrElse(Long.MaxValue)

      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByTagStage requires ActorMaterializer")
      }

      private def calculateToOffset(): UUID = {
        val to: Long = UUIDs.unixTimestamp(UUIDs.timeBased()) - settings.eventsByTagEventualConsistency.toMillis
        val tOff = if (to < toOffsetMillis) {
          // The eventual consistency delay is before the end of the query
          val u = UUIDs.endOf(to)
          if (log.isDebugEnabled) {
            log.debug("[{}]: New toOffset (EC): {}", stageUuid, formatOffset(u))
          }
          u
        } else {
          // The eventual consistency delay is after the end of the query
          if (log.isDebugEnabled) {
            log.debug("{}: New toOffset (End): {}", stageUuid, formatOffset(toOffset.get))
          }
          toOffset.get
        }
        tOff
      }

      private def updateToOffset(): Unit =
        updateStageState(_.copy(toOffset = calculateToOffset()))

      private def updateQueryState(state: QueryState): Unit =
        updateStageState(_.copy(state = state))

      implicit def ec: ExecutionContextExecutor = materializer.executionContext

      setHandler(out, this)

      val newResultSetCb = getAsyncCallback[Try[ResultSet]] {
        case Success(rs) =>
          if (!stageState.state.isInstanceOf[QueryInProgress]) {
            throw new IllegalStateException(s"New ResultSet when in unexpected state ${stageState.state}")
          }
          updateStageState(_.copy(state = QueryResult(rs)))
          tryPushOne()
        case Failure(e) =>
          log.warning("Cassandra query failed: {}", e)
          fail(out, e)
      }

      override def preStart(): Unit = {
        stageState = StageState(QueryIdle, fromOffset, calculateToOffset(), initialTagPidSequenceNrs, None, bucketSize)
        if (log.isInfoEnabled) {
          log.info(
            s"[{}]: EventsByTag query [${session.tag}] starting with EC delay {}ms: fromOffset [{}] toOffset [{}]",
            stageUuid,
            settings.eventsByTagEventualConsistency.toMillis,
            formatOffset(fromOffset),
            toOffset.map(formatOffset))
        }
        log.debug("[{}] Starting with tag pid sequence nrs [{}]", stageUuid, stageState.tagPidSequenceNrs)

        if (settings.pubsubNotification) {
          Try {
            getStageActor {
              case (_, publishedTag) =>
                if (publishedTag.equals(session.tag)) {
                  log.debug("[{}] Received pub sub tag update for our tag, initiating query", stageUuid)
                  if (settings.eventsByTagEventualConsistency == Duration.Zero) {
                    continue()
                  } else if (settings.eventsByTagEventualConsistency < settings.refreshInterval) {
                    // No point scheduling for EC if the QueryPoll will come in beforehand
                    scheduleOnce(TagNotification, settings.eventsByTagEventualConsistency)
                  }

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
            scheduleQueryPoll(initial, interval)
            log.debug("[{}] Scheduling query poll at: {} ms", stageUuid, interval.toMillis)
          case None =>
            log.debug("[{}] CurrentQuery: No query polling", stageUuid)
        }
      }

      @silent("deprecated")
      private def scheduleQueryPoll(initial: FiniteDuration, interval: FiniteDuration): Unit = {
        schedulePeriodicallyWithInitialDelay(QueryPoll, initial, interval)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case QueryPoll | TagNotification =>
          continue()
      }

      override def onPull(): Unit =
        tryPushOne()

      private def continue(): Unit =
        stageState.state match {
          case QueryIdle =>
            if (stageState.isLookingForMissing)
              lookForMissing()
            else
              query()

          case _: QueryResult | _: BufferedEvents =>
            tryPushOne()
          case _: QueryInProgress =>
        }

      private def query(): Unit = {
        updateToOffset()
        if (log.isDebugEnabled) {
          log.debug(
            "[{}] Executing query: timeBucket: {} from offset: {} to offset: {}",
            stageUuid,
            stageState.currentTimeBucket,
            formatOffset(stageState.fromOffset),
            formatOffset(stageState.toOffset))
        }
        updateStageState(_.copy(state = QueryInProgress()))
        session
          .selectEventsForBucket(stageState.currentTimeBucket, stageState.fromOffset, stageState.toOffset)
          .onComplete(newResultSetCb.invoke)
      }

      private def lookForMissing(): Unit = {
        val missing = stageState.missingLookup match {
          case Some(m) => m
          case None =>
            throw new IllegalStateException(
              s"lookingForMissingCalled for tag ${session.tag} when there " +
              s"is no missing. Raise a bug with debug logging.")
        }

        if (missing.deadline.isOverdue()) {
          abortMissingSearch(missing)
        } else {
          updateQueryState(QueryInProgress())
          if (log.isDebugEnabled) {
            log.debug(
              s"[${stageUuid}] " + s"${session.tag}: Executing query to look for {}. Timebucket: {}. From: {}. To: {}",
              if (missing.gapDetected) "missing" else "previous events",
              missing.bucket,
              formatOffset(missing.previousOffset),
              formatOffset(missing.maxOffset))
          }
          session
            .selectEventsForBucket(missing.bucket, missing.previousOffset, missing.maxOffset)
            .onComplete(newResultSetCb.invoke)
        }
      }

      private def abortMissingSearch(missing: LookingForMissing): Unit = {
        if (missing.gapDetected) {
          fail(
            out,
            new MissingTaggedEventException(
              session.tag,
              missing.persistenceId,
              missing.missing,
              Offset.timeBasedUUID(missing.previousOffset)))
        } else {
          log.debug(
            "[{}] [{}]: Finished scanning for older events for persistence id [{}]. Max pid sequence nr found [{}]",
            stageUuid,
            session.tag,
            missing.persistenceId,
            missing.maxSequenceNr)
          stopLookingForMissing(missing, missing.buffered)
          tryPushOne()
        }
      }

      def checkResultSetForMissing(rs: ResultSet, m: LookingForMissing): Unit = {
        val row = rs.one()
        // we only extract the event if it is the missing one we've looking for
        val rowPersistenceId = row.getString("persistence_id")
        val rowTagPidSequenceNr = row.getLong("tag_pid_sequence_nr")
        if (rowPersistenceId == m.persistenceId && m.missing.contains(rowTagPidSequenceNr)) {
          val uuidRow = extractUuidRow(row)
          val remainingEvents = m.missing - uuidRow.tagPidSequenceNr
          log.debug(
            "[{}] {}: Found a missing event, sequence nr {}. Remaining missing: {}",
            stageUuid,
            session.tag,
            uuidRow.tagPidSequenceNr,
            remainingEvents)
          if (remainingEvents.isEmpty) {
            stopLookingForMissing(m, uuidRow :: m.buffered)
          } else {
            log.debug("[{}] [{}]: There are more missing events. [{}]", stageUuid, session.tag, remainingEvents)
            stageState = stageState.copy(missingLookup = stageState.missingLookup.map(m =>
              m.copy(missing = remainingEvents, buffered = uuidRow :: m.buffered)))
          }
        }
      }

      private def updateStageState(f: StageState => StageState): Unit =
        stageState = f(stageState)

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
          updateStageState(
            _.copy(fromOffset = repr.offset).tagPidSequenceNumberUpdate(repr.persistenceId, (1, repr.offset)))
          push(out, repr)
          false
        } else if (usingOffset && (stageState.currentTimeBucket.inPast || settings.eventsByTagNewPersistenceIdScanTimeout == Duration.Zero)) {
          // If we're in the past and this is an offset query we assume this is
          // the first tagPidSequenceNr
          log.debug(
            "[{}] New persistence id: {}. Timebucket: {}. Tag pid sequence nr: {}",
            stageUuid,
            repr.persistenceId,
            stageState.currentTimeBucket,
            repr.tagPidSequenceNr)
          updateStageState(
            _.copy(fromOffset = repr.offset)
              .tagPidSequenceNumberUpdate(repr.persistenceId, (repr.tagPidSequenceNr, repr.offset)))
          push(out, repr)
          false
        } else {
          if (log.isDebugEnabled) {
            log.debug(
              s"[${stageUuid}] " + " [{}]: New persistence id: [{}] does not start at tag pid sequence nr 1. This could either be that the events are before the offset or that they are missing. Tag pid sequence nr found: [{}]. Looking for lower tag pid sequence nrs for [{}]",
              session.tag,
              repr.persistenceId,
              repr.tagPidSequenceNr,
              settings.eventsByTagNewPersistenceIdScanTimeout.pretty)
          }
          val previousBucketStart =
            UUIDs.startOf(stageState.currentTimeBucket.previous(1).key)
          val startingOffset: UUID =
            if (UUIDComparator.comparator.compare(previousBucketStart, fromOffset) < 0) {
              log.debug("[{}] Starting at fromOffset", stageUuid)
              fromOffset
            } else {
              log.debug("[{}] Starting at startOfBucket", stageUuid)
              previousBucketStart
            }
          val bucket = TimeBucket(startingOffset, bucketSize)
          val lookInPrevious = !bucket.within(startingOffset)
          updateStageState(
            _.copy(missingLookup = Some(LookingForMissing(
              repr :: Nil,
              startingOffset,
              bucket,
              lookInPrevious,
              repr.offset,
              repr.persistenceId,
              repr.tagPidSequenceNr,
              (1L until repr.tagPidSequenceNr).toSet,
              gapDetected = false,
              deadline = Deadline.now + settings.eventsByTagNewPersistenceIdScanTimeout))))
          true
        }
      }

      def handleExistingPersistenceId(repr: UUIDRow, lastSequenceNr: Long, lastUUID: UUID): Boolean = {
        val expectedSequenceNr = lastSequenceNr + 1
        val pid = repr.persistenceId
        if (repr.tagPidSequenceNr < expectedSequenceNr) {
          log.warning(
            s"[${stageUuid}] " + "Duplicate sequence number. Persistence id: {}. Tag: {}. Expected sequence nr: {}. " +
            "Actual {}. This will be dropped.",
            pid,
            session.tag,
            expectedSequenceNr,
            repr.tagPidSequenceNr)
          false
        } else if (repr.tagPidSequenceNr > expectedSequenceNr) {
          log.info(
            s"[${stageUuid}] " + "{}: Missing event for persistence id: {}. Expected sequence nr: {}, actual: {}.",
            session.tag,
            pid,
            expectedSequenceNr,
            repr.tagPidSequenceNr)
          val bucket = TimeBucket(repr.offset, bucketSize)
          val lookInPrevious = !bucket.within(lastUUID)
          updateStageState(
            _.copy(missingLookup = Some(LookingForMissing(
              repr :: Nil,
              lastUUID,
              bucket,
              lookInPrevious,
              repr.offset,
              repr.persistenceId,
              repr.tagPidSequenceNr,
              (expectedSequenceNr until repr.tagPidSequenceNr).toSet,
              gapDetected = true,
              deadline = Deadline.now + settings.eventsByTagGapTimeout))))
          true
        } else {
          // this is per row so put behind a flag. Per query logging is on at debug without this flag
          if (verboseDebug)
            log.debug(
              s"[${stageUuid}] " + " Updating offset to {} from pId {} seqNr {} tagPidSequenceNr {}",
              formatOffset(stageState.fromOffset),
              pid,
              repr.sequenceNr,
              repr.tagPidSequenceNr)

          updateStageState(
            _.copy(fromOffset = repr.offset)
              .tagPidSequenceNumberUpdate(repr.persistenceId, (expectedSequenceNr, repr.offset)))
          push(out, repr)
          false
        }
      }

      @tailrec def tryPushOne(): Unit =
        stageState.state match {
          case QueryResult(rs) if isAvailable(out) =>
            if (rs.isExhausted) {
              queryExhausted()
            } else if (rs.getAvailableWithoutFetching == 0) {
              log.debug("[{}] Fetching more", stageUuid)
              fetchMore(rs)
            } else if (stageState.isLookingForMissing) {
              checkResultSetForMissing(rs, stageState.missingLookup.get)
              tryPushOne()
            } else {
              val row = rs.one()
              val repr = extractUuidRow(row)
              val pid = repr.persistenceId

              val missing = stageState.tagPidSequenceNrs.get(pid) match {
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
              log.debug("[{}] Fully fetched, getting more for next pull (not implemented yet)", stageUuid)
            }
          case BufferedEvents(events) if isAvailable(out) =>
            log.debug("[{}] Pushing buffered event", stageUuid)
            events match {
              case Nil =>
                continue()
              case e :: Nil =>
                push(out, e)
                updateQueryState(QueryIdle)
                query()
              case e :: tail =>
                push(out, e)
                updateQueryState(BufferedEvents(tail))
                tryPushOne()
            }

          case _ =>
            log.debug(
              "[{}] Trying to push one but no query results currently {} or demand {}",
              stageUuid,
              stageState.state,
              isAvailable(out))
        }

      private def queryExhausted(): Unit = {
        updateQueryState(QueryIdle)

        if (log.isDebugEnabled) {
          log.debug(
            "[{}] Query exhausted, next query: from: {} to: {}",
            stageUuid,
            formatOffset(stageState.fromOffset),
            formatOffset(stageState.toOffset))
        }

        if (stageState.isLookingForMissing) {
          val missing = stageState.missingLookup.get
          if (missing.queryPrevious) {
            log.debug("[{}] [{}]: Missing could be in previous bucket. Querying right away.", stageUuid, session.tag)
            updateStageState(_.copy(missingLookup = stageState.missingLookup.map(m =>
              m.copy(queryPrevious = false, bucket = m.bucket.previous(1)))))
            lookForMissing()
          } else {
            val timeLeft = missing.deadline.timeLeft
            if (timeLeft <= Duration.Zero) {
              abortMissingSearch(missing)
            } else {
              log.debug(
                s"[${stageUuid}] [{}]: Still looking for {}. {}. Duration left for search: {}",
                session.tag,
                if (missing.gapDetected) "missing" else "previous events",
                stageState,
                timeLeft.pretty)
              updateStageState(_.copy(missingLookup = stageState.missingLookup.map(m => {
                val newBucket = TimeBucket(m.maxOffset, bucketSize)
                m.copy(bucket = newBucket, queryPrevious = !newBucket.within(m.previousOffset))
              })))

              // the new persistence-id scan time is typically much smaller than the refresh interval
              // so do not wait for the next refresh to look again
              if (timeLeft < settings.refreshInterval) {
                scheduleOnce(QueryPoll, timeLeft)
              } else if (!isLiveQuery()) {
                // a current query doesn't have a poll schedule one
                scheduleOnce(QueryPoll, settings.refreshInterval)
              }
            }

          }
        } else if (stageState.shouldMoveBucket()) {
          nextTimeBucket()
          log.debug("[{}] Moving to next bucket: {}", stageUuid, stageState.currentTimeBucket)
          query()
        } else {
          if (toOffset.contains(stageState.toOffset)) {
            log.debug("[{}] Current query finished and has passed the eventual consistency delay, ending.", stageUuid)
            completeStage()
          } else {
            log.debug("[{}] Nothing todo, waiting for next poll", stageUuid)
            if (!isLiveQuery()) {
              // As this isn't a live query schedule a poll for now
              log.debug(
                "[{}] Scheduling poll for eventual consistency delay: {}",
                stageUuid,
                settings.eventsByTagEventualConsistency)
              scheduleOnce(QueryPoll, settings.eventsByTagEventualConsistency)
            }
          }
        }
      }

      private def stopLookingForMissing(m: LookingForMissing, buffered: List[UUIDRow]): Unit = {
        updateQueryState(if (buffered.isEmpty) {
          QueryIdle
        } else {
          BufferedEvents(buffered.sortBy(_.tagPidSequenceNr))
        })
        log.debug("[{}] Search over. Sending buffered events. {}", stageUuid, stageState.state)
        updateStageState(
          _.copy(fromOffset = m.maxOffset, missingLookup = None)
            .tagPidSequenceNumberUpdate(m.persistenceId, (m.maxSequenceNr, m.maxOffset)))
      }

      private def fetchMore(rs: ResultSet): Unit = {
        log.debug("[{}] No more results without paging. Requesting more.", stageUuid)
        val moreResults: Future[ResultSet] = rs.fetchMoreResults().asScala
        updateQueryState(QueryInProgress())
        moreResults.onComplete(newResultSetCb.invoke)
      }

      private def extractUuidRow(row: Row): UUIDRow =
        UUIDRow(
          persistenceId = row.getString("persistence_id"),
          sequenceNr = row.getLong("sequence_nr"),
          offset = row.getUUID("timestamp"),
          tagPidSequenceNr = row.getLong("tag_pid_sequence_nr"),
          row)

      private def nextTimeBucket(): Unit = {
        updateStageState(_.copy(fromOffset = UUIDs.startOf(stageState.currentTimeBucket.next().key)))
        updateToOffset()
      }
    }
}
