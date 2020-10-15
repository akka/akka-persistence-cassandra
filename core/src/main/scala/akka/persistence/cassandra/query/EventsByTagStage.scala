/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.annotation.InternalApi
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.EventsByTagStage._
import akka.stream.stage.{ GraphStage, _ }
import akka.stream.{ ActorMaterializer, Attributes, Outlet, SourceShape }
import akka.util.PrettyDuration._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success, Try }
import java.lang.{ Long => JLong }

import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.EventByTagStatements
import akka.util.UUIDComparator
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.github.ghik.silencer.silent

import scala.compat.java8.FutureConverters._

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
      settings: PluginSettings,
      refreshInterval: Option[FiniteDuration],
      bucketSize: BucketSize,
      usingOffset: Boolean,
      initialTagPidSequenceNrs: Map[Tag, (TagPidSequenceNr, UUID)],
      scanner: TagViewSequenceNumberScanner): EventsByTagStage =
    new EventsByTagStage(
      session,
      fromOffset,
      toOffset,
      settings,
      refreshInterval,
      bucketSize,
      usingOffset,
      initialTagPidSequenceNrs,
      scanner)

  private[akka] class TagStageSession(
      val tag: String,
      readProfile: String,
      session: CqlSession,
      statements: EventByTagStatements) {
    def selectEventsForBucket(bucket: TimeBucket, from: UUID, to: UUID): Future[AsyncResultSet] = {
      val bound =
        statements.byTagWithUpperLimit.bind(tag, bucket.key: JLong, from, to).setExecutionProfileName(readProfile)

      session.executeAsync(bound).toScala
    }
  }

  private sealed trait QueryState
  private case object QueryIdle extends QueryState
  private final case class QueryInProgress(abortForMissingSearch: Boolean, startTime: Long = System.nanoTime())
      extends QueryState
  private final case class QueryResult(resultSet: AsyncResultSet) extends QueryState
  private final case class BufferedEvents(events: List[UUIDRow]) extends QueryState {
    override def toString: String = s"BufferedEvents(${events.size})"
  }

  type Offset = UUID
  type MaxSequenceNr = Long

  final case class MissingData(maxOffset: UUID, maxSequenceNr: TagPidSequenceNr)

  /**
   *
   * @param queryPrevious Should the previous bucket be queries right away. Searches go back one bucket, first the current bucket then
   *                      the previous bucket. This is repeated every refresh interval.
   * @param gapDetected Whether an explicit gap has been detected e.g. events 1-4 have been seen then the next event is not 5.
   *                    The other scenario is that the first event for a persistence id is not 1, which when starting from an offset
   *                    won't fail the stream as this is normal. However due to the eventual consistency of C* the stream still looks for earlier
   *                    events for a short time.
   */
  private final case class LookingForMissing(
      buffered: List[UUIDRow],
      minOffset: UUID,
      maxOffset: UUID,
      bucket: TimeBucket,
      queryPrevious: Boolean,
      missingData: Map[PersistenceId, MissingData],
      remainingMissing: Map[PersistenceId, Set[Long]],
      deadline: Deadline,
      gapDetected: Boolean) {

    // don't include buffered in the toString
    override def toString =
      s"LookingForMissing{min=$minOffset maxOffset=$maxOffset bucket=$bucket " +
      s"queryPrevious=$queryPrevious searchingFor=${remainingMissing} " +
      s"missing=$remainingMissing deadline=$deadline gapDetected=$gapDetected"
  }

  private sealed trait QueryPoll
  private case object PeriodicQueryPoll extends QueryPoll
  private case object OneOffQueryPoll extends QueryPoll
  private final case class TagNotification(resolution: Long) extends QueryPoll
  private case object PersistenceIdsCleanup
  private case object ScanForDelayedEvents

  type LastUpdated = Long

  private case class StageState(
      state: QueryState,
      // updated each time a new row is read, may skip delayed events with lower offsets
      // these gaps will be detected for the same pid, for cross pid delayed events it
      // relies on the eventual consistency delay
      fromOffset: UUID,
      // upper limit for next query. Always kept at least the eventual consistency in the past
      toOffset: UUID,
      // for each persistence id what is the latest tag pid sequenceNr number and offset
      tagPidSequenceNrs: Map[PersistenceId, (TagPidSequenceNr, UUID, LastUpdated)],
      delayedScanInProgress: Boolean,
      previousLongDelayedScan: Long,
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

    def tagPidSequenceNumberUpdate(
        pid: PersistenceId,
        tagPidSequenceNr: (TagPidSequenceNr, UUID, LastUpdated)): StageState =
      copy(tagPidSequenceNrs = tagPidSequenceNrs + (pid -> tagPidSequenceNr))

    // override to give nice offset formatting
    override def toString: String =
      s"""StageState(state: $state, fromOffset: ${formatOffset(fromOffset)}, toOffset: ${formatOffset(toOffset)}, tagPidSequenceNrs: $tagPidSequenceNrs, missingLookup: $missingLookup, bucketSize: $bucketSize)"""
  }

  private val uuidRowOrdering = new Ordering[UUIDRow] {
    override def compare(x: UUIDRow, y: UUIDRow): Int = {
      val offsetCompare = UUIDComparator.comparator.compare(x.offset, y.offset)
      if (offsetCompare == 0)
        Ordering.Long.compare(x.tagPidSequenceNr, y.tagPidSequenceNr)
      else
        offsetCompare
    }
  }

}

/** INTERNAL API */
@InternalApi private[akka] class EventsByTagStage(
    session: TagStageSession,
    initialQueryOffset: UUID,
    toOffset: Option[UUID],
    settings: PluginSettings,
    refreshInterval: Option[FiniteDuration],
    bucketSize: BucketSize,
    usingOffset: Boolean,
    initialTagPidSequenceNrs: Map[PersistenceId, (TagPidSequenceNr, UUID)],
    scanner: TagViewSequenceNumberScanner)
    extends GraphStage[SourceShape[UUIDRow]] {

  import settings.querySettings
  import settings.eventsByTagSettings

  private val out: Outlet[UUIDRow] = Outlet("event.out")
  private val verboseDebug = eventsByTagSettings.verboseDebug
  private val backtracking = eventsByTagSettings.backtrack

  override def shape = SourceShape(out)

  val stageUuid = UUID.randomUUID().toString

  private def isLiveQuery(): Boolean = refreshInterval.isDefined

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging with OutHandler {
      override protected def logSource = classOf[EventsByTagStage]

      var stageState: StageState = _
      val toOffsetMillis =
        toOffset.map(Uuids.unixTimestamp).getOrElse(Long.MaxValue)

      lazy val system = materializer match {
        case a: ActorMaterializer => a.system
        case _ =>
          throw new IllegalStateException("EventsByTagStage requires ActorMaterializer")
      }

      private def calculateToOffset(): UUID = {
        val to: Long = Uuids.unixTimestamp(Uuids.timeBased()) - eventsByTagSettings.eventualConsistency.toMillis
        val tOff = if (to < toOffsetMillis) {
          // The eventual consistency delay is before the end of the query
          val u = Uuids.endOf(to)
          if (verboseDebug && log.isDebugEnabled) {
            log.debug("[{}]: New toOffset (EC): {}", stageUuid, formatOffset(u))
          }
          u
        } else {
          // The eventual consistency delay is after the end of the query
          if (verboseDebug && log.isDebugEnabled) {
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

      val newResultSetCb = getAsyncCallback[Try[AsyncResultSet]] {
        case Success(rs) =>
          val queryState = stageState.state match {
            case qip: QueryInProgress => qip
            case _ =>
              throw new IllegalStateException(s"New ResultSet when in unexpected state ${stageState.state}")
          }
          if (queryState.abortForMissingSearch) {
            lookForMissing()
          } else {
            updateStageState(_.copy(state = QueryResult(rs)))
            tryPushOne()
          }
        case Failure(e) =>
          log.warning("Cassandra query failed: {}", e.getMessage)
          fail(out, e)
      }

      val backTrackCb = getAsyncCallback[Try[Map[PersistenceId, (TagPidSequenceNr, UUID)]]] {
        case Failure(e) =>
          updateStageState(_.copy(delayedScanInProgress = false))
          log.warning("Backtrack failed, this will retried. {}", e)
        case Success(sequenceNrs) =>
          updateStageState(_.copy(delayedScanInProgress = false))
          log.debug("Current sequence nrs: {} from back tracking: {}", stageState.tagPidSequenceNrs, sequenceNrs)

          val withDelayedEvents: Map[PersistenceId, (TagPidSequenceNr, Offset)] = sequenceNrs.filter {
            case (pid, (tagPidSequenceNr, _)) =>
              stageState.tagPidSequenceNrs.get(pid) match {
                case Some((currentTagPidSequenceNr, _, _)) if currentTagPidSequenceNr < tagPidSequenceNr =>
                  // the current can be bigger than the scanned sequence nrs as the stage continues querying
                  // but if the current is lower than the tag pid sequence nr then there is a missed event
                  true
                case None =>
                  // a pid that has never been found before
                  true
                case _ =>
                  false
              }
          }

          // TODO: the looking for missing currently tries to find all the events that are missing. What if there is
          // a large number? Could either fail or change the missing search to find them in chunks and deliver them,
          // this would mean changing the missing search to find them in order, right now all missing events are found
          // and then sorted then delivered.

          if (withDelayedEvents.nonEmpty && !stageState.isLookingForMissing) {
            log.debug(
              "The following persistence ids have delayed events: {}. Initiating search for the events.",
              withDelayedEvents)

            val existingBufferedEvents = stageState.state match {
              case BufferedEvents(events) => events
              case _                      => Nil
            }

            val missingData = withDelayedEvents.transform {
              case (_, (delayedMaxTagPidSequenceNr, delayedMaxOffset)) =>
                MissingData(delayedMaxOffset, delayedMaxTagPidSequenceNr)
            }

            val missingSequenceNrs = withDelayedEvents.transform {
              case (delayedPid, (delayedMaxTagPidSequenceNr, _)) =>
                val fromSeqNr: Long = stageState.tagPidSequenceNrs.get(delayedPid).map(_._1).getOrElse(0L) + 1L
                (fromSeqNr to delayedMaxTagPidSequenceNr).toSet
            }

            val minOffset =
              Uuids.startOf(missingData.keys.foldLeft(Uuids.unixTimestamp(stageState.toOffset)) {
                case (acc, pid) =>
                  math.min(
                    stageState.tagPidSequenceNrs
                      .get(pid)
                      .map(t => Uuids.unixTimestamp(t._2))
                      .getOrElse(
                        // for new persistence ids look all the way back to the start of the previous bucket
                        stageState.currentTimeBucket.previous(1).key),
                    acc)
              })

            log.debug("Starting search for: {}", missingSequenceNrs)
            setLookingForMissingState(
              existingBufferedEvents,
              minOffset,
              stageState.toOffset,
              missingData,
              missingSequenceNrs,
              explicitGapDetected = true,
              eventsByTagSettings.eventsByTagGapTimeout)

            val total = totalMissing()
            if (total > settings.eventsByTagSettings.maxMissingToSearch) {
              failTooManyMissing(total)
            } else {
              stageState.state match {
                case qip @ QueryInProgress(_, _) =>
                  // let the current query finish and then look for missing
                  updateStageState(_.copy(state = qip.copy(abortForMissingSearch = true)))
                case _ =>
                  lookForMissing()
              }
            }
          }
      }

      override def preStart(): Unit = {
        stageState = StageState(QueryIdle, initialQueryOffset, calculateToOffset(), initialTagPidSequenceNrs.transform {
          case (_, (tagPidSequenceNr, offset)) => (tagPidSequenceNr, offset, System.currentTimeMillis())
        }, delayedScanInProgress = false, System.currentTimeMillis(), None, bucketSize)
        if (log.isInfoEnabled) {
          log.info(
            s"[{}]: EventsByTag query [${session.tag}] starting with EC delay {}ms: fromOffset [{}] toOffset [{}]",
            stageUuid,
            eventsByTagSettings.eventualConsistency.toMillis,
            formatOffset(initialQueryOffset),
            toOffset.map(formatOffset))
        }
        log.debug("[{}] Starting with tag pid sequence nrs [{}]", stageUuid, stageState.tagPidSequenceNrs)

        if (eventsByTagSettings.pubsubNotification.isFinite) {
          Try {
            getStageActor {
              case (_, publishedTag) =>
                if (publishedTag.equals(session.tag)) {
                  log.debug("[{}] Received pub sub tag update for our tag, initiating query", stageUuid)
                  if (eventsByTagSettings.eventualConsistency == Duration.Zero) {
                    continue()
                  } else if (eventsByTagSettings.eventualConsistency < querySettings.refreshInterval) {
                    // No point scheduling for EC if the QueryPoll will come in beforehand
                    // No point in scheduling more frequently than once every 10 ms
                    scheduleOnce(
                      TagNotification(System.currentTimeMillis() / 10),
                      eventsByTagSettings.eventualConsistency)
                  }

                }
            }
            DistributedPubSub(system).mediator !
            DistributedPubSubMediator.Subscribe(s"apc.tags.${session.tag}", this.stageActor.ref)
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

        def optionallySchedule(key: Any, duration: Option[FiniteDuration]): Unit = {
          duration.foreach(fd => scheduleWithFixedDelay(key, fd, fd))
        }

        optionallySchedule(PersistenceIdsCleanup, eventsByTagSettings.cleanUpPersistenceIds)
        optionallySchedule(ScanForDelayedEvents, eventsByTagSettings.backtrack.interval)
      }

      @silent("deprecated")
      private def scheduleQueryPoll(initial: FiniteDuration, interval: FiniteDuration): Unit = {
        schedulePeriodicallyWithInitialDelay(PeriodicQueryPoll, initial, interval)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case _: QueryPoll =>
          continue()
        case PersistenceIdsCleanup =>
          cleanup()
        case ScanForDelayedEvents =>
          scanForDelayedEvents()
      }

      override def onPull(): Unit = {
        tryPushOne()
      }

      private def scanForDelayedEvents(): Unit = {
        if (!stageState.delayedScanInProgress) {
          updateStageState(_.copy(delayedScanInProgress = true))

          val startOfPreviousBucket = stageState.currentTimeBucket.previous(1).key
          val fromOffsetTime = Uuids.unixTimestamp(stageState.fromOffset)
          val startOfScan =
            if (System.currentTimeMillis() - stageState.previousLongDelayedScan > backtracking.longIntervalMillis()) {
              val from = Uuids.startOf(backtracking.longPeriodMillis(fromOffsetTime, startOfPreviousBucket))
              log.debug("Initialising long period back track from {}", formatOffset(from))
              updateStageState(_.copy(previousLongDelayedScan = System.currentTimeMillis()))
              from
            } else {
              val from = Uuids.startOf(backtracking.periodMillis(fromOffsetTime, startOfPreviousBucket))
              if (verboseDebug)
                log.debug("Initialising period back track from {}", formatOffset(from))
              from
            }

          // don't scan from before the query fromOffset
          val scanFrom =
            if (UUIDComparator.comparator.compare(startOfScan, initialQueryOffset) < 0) initialQueryOffset
            else startOfScan
          // to the current from offset to avoid any events being found that will be found by this stage during its querying
          val scanTo = stageState.fromOffset
          // find the max sequence nr for each persistence id up until the current offset, if any of these are
          // higher than the current sequence nr for each persistence id they have been missed due to a delay with
          // eventual consistency set too low or events being delayed significantly
          scanner
            .scan(session.tag, scanFrom, scanTo, bucketSize, Duration.Zero, math.max)
            .onComplete(backTrackCb.invoke)
        }
      }

      private val cleanupPersistenceIdsMills =
        eventsByTagSettings.cleanUpPersistenceIds.map(_.toMillis).getOrElse(Long.MaxValue)
      private def cleanup(): Unit = {
        val now = System.currentTimeMillis()
        val remaining = stageState.tagPidSequenceNrs.filterNot {
          case (_, (_, _, lastUpdated)) =>
            (now - lastUpdated) > cleanupPersistenceIdsMills
        }
        updateStageState(_.copy(tagPidSequenceNrs = remaining))
      }

      private def continue(): Unit = {
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
      }

      private def query(): Unit = {
        updateToOffset()
        if (verboseDebug && log.isDebugEnabled) {
          log.debug(
            "[{}] Executing query: timeBucket: {} from offset: {} to offset: {}",
            stageUuid,
            stageState.currentTimeBucket,
            formatOffset(stageState.fromOffset),
            formatOffset(stageState.toOffset))
        }
        updateStageState(_.copy(state = QueryInProgress(abortForMissingSearch = false)))
        session
          .selectEventsForBucket(stageState.currentTimeBucket, stageState.fromOffset, stageState.toOffset)
          .onComplete(newResultSetCb.invoke)
      }

      private def getMissingLookup(): LookingForMissing = {
        stageState.missingLookup match {
          case Some(m) => m
          case None =>
            throw new IllegalStateException(
              s"lookingForMissingCalled for tag ${session.tag} when there " +
              s"is no missing. Raise a bug with debug logging.")
        }
      }

      private def lookForMissing(): Unit = {
        val missing = getMissingLookup()
        if (missing.deadline.isOverdue()) {
          abortMissingSearch(missing)
        } else {
          updateQueryState(QueryInProgress(abortForMissingSearch = false))
          if (log.isDebugEnabled) {
            log.debug(
              s"[${stageUuid}] " + s"${session.tag}: Executing query to look for {}. Timebucket: {}. From: {}. To: {}",
              if (missing.gapDetected) "missing" else "previous events",
              missing.bucket,
              formatOffset(missing.minOffset),
              formatOffset(missing.maxOffset))
          }
          session
            .selectEventsForBucket(missing.bucket, missing.minOffset, missing.maxOffset)
            .onComplete(newResultSetCb.invoke)
        }
      }

      private def abortMissingSearch(missing: LookingForMissing): Unit = {
        if (missing.gapDetected) {
          fail(
            out,
            new MissingTaggedEventException(
              session.tag,
              missing.remainingMissing,
              missing.minOffset,
              missing.maxOffset))
        } else {
          log.debug(
            "[{}] [{}]: Finished scanning for older events for persistence ids [{}]",
            stageUuid,
            session.tag,
            missing.remainingMissing.keys.mkString(","))
          stopLookingForMissing(missing, missing.buffered)
          tryPushOne()
        }
      }

      def checkResultSetForMissing(rs: AsyncResultSet, m: LookingForMissing): Unit = {
        val row = rs.one()
        // we only extract the event if it is the missing one we've looking for
        val rowPersistenceId = row.getString("persistence_id")
        val rowTagPidSequenceNr = row.getLong("tag_pid_sequence_nr")

        m.remainingMissing.get(rowPersistenceId) match {
          case Some(remainingMissing) if remainingMissing.contains(rowTagPidSequenceNr) =>
            val uuidRow = extractUuidRow(row)
            val remainingMissingSequenceNrs = remainingMissing - uuidRow.tagPidSequenceNr
            val remainingEvents =
              if (remainingMissingSequenceNrs.isEmpty)
                m.remainingMissing - rowPersistenceId
              else
                m.remainingMissing.updated(rowPersistenceId, remainingMissingSequenceNrs)
            if (verboseDebug) {
              log.debug(
                "[{}] {}: Found a missing event, sequence nr {}. Remaining missing: {}",
                stageUuid,
                session.tag,
                uuidRow.tagPidSequenceNr,
                remainingEvents)
            }
            if (remainingEvents.isEmpty) {
              stopLookingForMissing(m, uuidRow :: m.buffered)
            } else {
              log.debug("[{}] [{}]: There are more missing events. [{}]", stageUuid, session.tag, remainingEvents)
              stageState = stageState.copy(missingLookup = stageState.missingLookup.map(m =>
                m.copy(remainingMissing = remainingEvents, buffered = uuidRow :: m.buffered)))
            }
          case _ =>
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
            _.copy(fromOffset = repr.offset)
              .tagPidSequenceNumberUpdate(repr.persistenceId, (1, repr.offset, System.currentTimeMillis())))
          push(out, repr)
          false
        } else if (usingOffset && (stageState.currentTimeBucket.inPast || eventsByTagSettings.newPersistenceIdScanTimeout == Duration.Zero)) {
          // If we're in the past and this is an offset query we assume this is
          // the first tagPidSequenceNr
          log.debug(
            "[{}] New persistence id: {}. Timebucket: {}. Tag pid sequence nr: {}",
            stageUuid,
            repr.persistenceId,
            stageState.currentTimeBucket,
            repr.tagPidSequenceNr)
          updateStageState(
            _.copy(fromOffset = repr.offset).tagPidSequenceNumberUpdate(
              repr.persistenceId,
              (repr.tagPidSequenceNr, repr.offset, System.currentTimeMillis())))
          push(out, repr)
          false
        } else {
          if (log.isDebugEnabled) {
            log.debug(
              s"[${stageUuid}] " + " [{}]: Persistence Id not in metadata: [{}] does not start at tag pid sequence nr 1. " +
              "This could either be that the events are before the offset, that the metadata has been dropped or that they are delayed. " +
              "Tag pid sequence nr found: [{}]. Looking for lower tag pid sequence nrs for [{}] in the current and previous buckets.",
              session.tag,
              repr.persistenceId,
              repr.tagPidSequenceNr,
              eventsByTagSettings.newPersistenceIdScanTimeout.pretty)
          }
          val previousBucketStart =
            Uuids.startOf(stageState.currentTimeBucket.previous(1).key)

          val startingOffset: UUID =
            if (UUIDComparator.comparator.compare(previousBucketStart, initialQueryOffset) < 0) {
              log.debug("[{}] Starting at fromOffset", stageUuid)
              initialQueryOffset
            } else {
              log.debug("[{}] Starting at startOfBucket", stageUuid)
              previousBucketStart
            }
          val missingData = Map(repr.persistenceId -> MissingData(repr.offset, repr.tagPidSequenceNr))

          setLookingForMissingState(
            repr :: Nil,
            startingOffset,
            repr.offset,
            missingData,
            Map(repr.persistenceId -> (1L until repr.tagPidSequenceNr).toSet),
            explicitGapDetected = false,
            eventsByTagSettings.newPersistenceIdScanTimeout)
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
          val missingData = Map(repr.persistenceId -> MissingData(repr.offset, repr.tagPidSequenceNr))
          setLookingForMissingState(
            repr :: Nil,
            lastUUID,
            repr.offset,
            missingData,
            Map(repr.persistenceId -> (expectedSequenceNr until repr.tagPidSequenceNr).toSet),
            explicitGapDetected = true,
            eventsByTagSettings.eventsByTagGapTimeout)
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
            _.copy(fromOffset = repr.offset).tagPidSequenceNumberUpdate(
              repr.persistenceId,
              (expectedSequenceNr, repr.offset, System.currentTimeMillis())))
          push(out, repr)
          false
        }
      }

      private def setLookingForMissingState(
          events: List[UUIDRow],
          fromOffset: UUID,
          toOffset: UUID,
          missingOffsets: Map[PersistenceId, MissingData],
          missingSequenceNrs: Map[PersistenceId, Set[Long]],
          explicitGapDetected: Boolean,
          timeout: FiniteDuration): Unit = {

        // Start in the toOffsetBucket as it is considered more likely an event has been missed due to an event
        // being delayed slightly rather than by a full bucket
        val bucket = TimeBucket(toOffset, bucketSize)
        // Could the event be in the previous bucket?
        val lookInPrevious = shouldSearchInPreviousBucket(bucket, fromOffset)
        updateStageState(
          _.copy(
            missingLookup = Some(
              LookingForMissing(
                events,
                fromOffset,
                toOffset,
                bucket,
                lookInPrevious,
                missingOffsets,
                missingSequenceNrs,
                Deadline.now + timeout,
                explicitGapDetected))))
      }

      private def totalMissing(): Long = {
        val missing = getMissingLookup()
        missing.remainingMissing.values.foldLeft(0L)(_ + _.size)
      }
      private def failTooManyMissing(total: Long): Unit = {
        failStage(
          new RuntimeException(s"$total missing tagged events for tag [${session.tag}]. Failing without search"))
      }

      @tailrec def tryPushOne(): Unit =
        stageState.state match {
          case QueryResult(rs) if isAvailable(out) =>
            if (isExhausted(rs)) {
              queryExhausted()
            } else if (rs.remaining() == 0) {
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
                case Some((lastSequenceNr, lastUUID, _)) =>
                  handleExistingPersistenceId(repr, lastSequenceNr, lastUUID)
              }

              if (missing) {
                val total = totalMissing()
                if (total > settings.eventsByTagSettings.maxMissingToSearch) {
                  failTooManyMissing(total)
                } else {
                  lookForMissing()
                }
              } else {
                tryPushOne()
              }
            }
          case QueryResult(rs) =>
            if (isExhausted(rs)) {
              queryExhausted()
            } else if (rs.remaining() == 0) {
              log.debug("[{}] Fully fetched, getting more for next pull (not implemented yet)", stageUuid)
            }
          case BufferedEvents(events) if isAvailable(out) =>
            if (verboseDebug)
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
            if (verboseDebug)
              log.debug(
                "[{}] Trying to push one but no query results currently {} or demand {}",
                stageUuid,
                stageState.state,
                isAvailable(out))
        }

      private def shouldSearchInPreviousBucket(bucket: TimeBucket, fromOffset: UUID): Boolean =
        !bucket.within(fromOffset)

      private def queryExhausted(): Unit = {
        updateQueryState(QueryIdle)

        if (verboseDebug && log.isDebugEnabled) {
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
                m.copy(bucket = newBucket, queryPrevious = shouldSearchInPreviousBucket(newBucket, m.minOffset))
              })))

              // the new persistence-id scan time is typically much smaller than the refresh interval
              // so do not wait for the next refresh to look again
              if (timeLeft < querySettings.refreshInterval) {
                log.debug("Scheduling one off poll in {}", timeLeft.pretty)
                scheduleOnce(OneOffQueryPoll, timeLeft)
              } else if (!isLiveQuery()) {
                // a current query doesn't have a poll schedule one
                log.debug("Scheduling one off poll in {}", querySettings.refreshInterval)
                scheduleOnce(OneOffQueryPoll, querySettings.refreshInterval)
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
            if (verboseDebug)
              log.debug("[{}] Nothing todo, waiting for next poll", stageUuid)
            if (!isLiveQuery()) {
              // As this isn't a live query schedule a poll for now
              log.debug(
                "[{}] Scheduling poll for eventual consistency delay: {}",
                stageUuid,
                eventsByTagSettings.eventualConsistency)
              scheduleOnce(OneOffQueryPoll, eventsByTagSettings.eventualConsistency)
            }
          }
        }
      }

      private def stopLookingForMissing(m: LookingForMissing, buffered: List[UUIDRow]): Unit = {
        updateQueryState(if (buffered.isEmpty) {
          QueryIdle
        } else {
          BufferedEvents(buffered.sorted(uuidRowOrdering))
        })
        log.debug("[{}] Search over. Buffered events ready for delivery: {}", stageUuid, stageState.state)

        // set all the tag pid sequence nrs to the offset of the events found in the missing search. This is important
        // if another delayed event scan happens before all the events are delivered
        updateStageState(oldState => {
          m.missingData.foldLeft(oldState.copy(fromOffset = m.maxOffset, missingLookup = None)) {
            case (acc, (pid, missingData)) =>
              log.debug("Updating tag pid sequence nr for pid {} to {}", pid, missingData.maxSequenceNr)
              acc.tagPidSequenceNumberUpdate(
                pid,
                (missingData.maxSequenceNr, missingData.maxOffset, System.currentTimeMillis()))
          }
        })
      }

      private def fetchMore(rs: AsyncResultSet): Unit = {
        log.debug("[{}] No more results without paging. Requesting more.", stageUuid)
        val moreResults = rs.fetchNextPage().toScala
        updateQueryState(QueryInProgress(abortForMissingSearch = false))
        moreResults.onComplete(newResultSetCb.invoke)
      }

      private def extractUuidRow(row: Row): UUIDRow =
        UUIDRow(
          persistenceId = row.getString("persistence_id"),
          sequenceNr = row.getLong("sequence_nr"),
          offset = row.getUuid("timestamp"),
          tagPidSequenceNr = row.getLong("tag_pid_sequence_nr"),
          row)

      private def nextTimeBucket(): Unit = {
        updateStageState(_.copy(fromOffset = Uuids.startOf(stageState.currentTimeBucket.next().key)))
        updateToOffset()
      }
    }
}
