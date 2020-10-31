/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.dispatch.ExecutionContexts
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.AllEventsOffset
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.JavaDurationConverters._
import akka.util.Timeout

// FIXME this can probably be implemented in akka-persistence-query
// only needs `persistenceIds` and `currentEventsByPersistenceId`

object AllEvents {
  import PersistenceIdsState._

  implicit val askTimeout: Timeout = 10.seconds

  private val actorNameCounter = new AtomicLong()

  def topicNameFromPersistenceId(pid: PersistenceId, numberOfSlices: Int): String = {
    val entityName = {
      val i = pid.indexOf('|')
      if (i >= 0) pid.substring(0, i)
      else "all"
    }
    val slice = math.abs(pid.hashCode % numberOfSlices)
    topicName(entityName, slice)
  }

  def topicName(entityName: String, slice: Int): String =
    s"apc.events.$entityName-$slice"

  // FIXME haven't thought about the real API here yet
  def allEvents(
      system: ActorSystem,
      queries: CassandraReadJournal,
      offset: AllEventsOffset,
      entityName: String,
      slice: Int,
      numberOfSlices: Int): Source[EventEnvelope, NotUsed] = {
    allEvents(
      queries,
      matchingPersistenceId = pid => math.abs(pid.hashCode % numberOfSlices) == slice,
      offset,
      subscribeForDirectEvents = ref =>
        DistributedPubSub(system).mediator ! DistributedPubSubMediator.Subscribe(topicName(entityName, slice), ref))
  }

  def allEvents(
      queries: CassandraReadJournal,
      matchingPersistenceId: PersistenceId => Boolean,
      offset: AllEventsOffset,
      subscribeForDirectEvents: ActorRef => Unit): Source[EventEnvelope, NotUsed] = {

    def findAndEmitEvents(
        persistenceIdsState: ActorRef,
        pid: PersistenceId,
        seqNr: SeqNr): Source[EventEnvelope, NotUsed] = {
      queries
        .currentEventsByPersistenceId(pid, seqNr + 1, Long.MaxValue)
        .alsoTo(Sink.lastOption.mapMaterializedValue { lastFut =>
          // when the persistenceIdsState actor has handed out a persistenceId it will not emit that again until
          // it receives the updated seqNr
          lastFut.map {
            case None => persistenceIdsState ! UpdatePersistenceIdOffset(pid, seqNr, directEvents = false)
            case Some(last) =>
              persistenceIdsState ! UpdatePersistenceIdOffset(last.persistenceId, last.sequenceNr, directEvents = false)
          }(ExecutionContexts.parasitic)
          NotUsed
        })
    }

    def emitEvents(persistenceIdsState: ActorRef, events: Vector[PersistentRepr]): Source[EventEnvelope, NotUsed] = {
      val last = events.last
      persistenceIdsState ! UpdatePersistenceIdOffset(last.persistenceId, last.sequenceNr, directEvents = true)
      Source(
        events.map(
          repr =>
            EventEnvelope(
              Offset.sequence(repr.sequenceNr),
              repr.persistenceId,
              repr.sequenceNr,
              repr.payload,
              repr.timestamp)))
    }

    Source
      .fromMaterializer { (mat, _) =>
        val system = mat.system.asInstanceOf[ExtendedActorSystem]
        implicit val ec: ExecutionContext = mat.executionContext

        val name = s"persistenceIdsState-${actorNameCounter.incrementAndGet()}"
        val killSwitch = KillSwitches.shared(name)

        val runCurrentPersistenceIdQuery: ActorRef => Unit = { sendTo =>
          queries
            .currentPersistenceIds()
            .via(killSwitch.flow)
            .filterNot(offset.offsets.contains) // already known
            .filter(matchingPersistenceId)
            .groupedWithin(1000, 1.second)
            .map(PersistenceIdsState.PidsFromQuery.apply)
            .ask[Done](10)(sendTo)
            .runWith(Sink.ignore)(mat)
            .onComplete(_ => sendTo ! PersistenceIdsState.PidsQueryCompleted)
        }

        val persistenceIdsState: ActorRef = system.systemActorOf(
          PersistenceIdsState.props(offset, matchingPersistenceId, runCurrentPersistenceIdQuery),
          name)
        subscribeForDirectEvents(persistenceIdsState)

        Source
          .repeat(GetNextPersistenceId)
          .ask[NextPersistenceId](1)(persistenceIdsState)
          .flatMapConcat {
            case NextPersistenceId(pid, seqNr, events) =>
              if (pid == "")
                Source.empty
              else if (events.isEmpty)
                findAndEmitEvents(persistenceIdsState, pid, seqNr)
              else
                emitEvents(persistenceIdsState, events)
          }
          .watchTermination()(Keep.right)
          .mapMaterializedValue { done =>
            done.onComplete { _ =>
              killSwitch.shutdown()
              persistenceIdsState ! PoisonPill
            }
            NotUsed
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  }

}

object PersistenceIdsState {
  type PersistenceId = String
  type SeqNr = Long

  case object GetNextPersistenceId
  final case class NextPersistenceId(pid: PersistenceId, seqNr: SeqNr, events: Vector[PersistentRepr])
  final case class UpdatePersistenceIdOffset(pid: PersistenceId, seqNr: SeqNr, directEvents: Boolean)

  case object CurrentPersistenceIdsQueryTick
  final case class PidsFromQuery(pids: immutable.Seq[PersistenceId])
  case object PidsQueryCompleted

  sealed trait CurrentPersistenceIdsQueryState
  object CurrentPersistenceIdsQueryIdle extends CurrentPersistenceIdsQueryState
  object CurrentPersistenceIdsQueryInProgress extends CurrentPersistenceIdsQueryState
  object UseAllPidsNextTime extends CurrentPersistenceIdsQueryState

  private object PidState {
    val empty: PidState = PidState(0L, Vector.empty, inFlight = false, lru = false)
  }
  private case class PidState(seqNr: SeqNr, bufferedDirect: Vector[PersistentRepr], inFlight: Boolean, lru: Boolean)

  def props(
      offset: AllEventsOffset,
      matchingPersistenceId: PersistenceIdsState.PersistenceId => Boolean,
      runCurrentPersistenceIdQuery: ActorRef => Unit): Props =
    Props(new PersistenceIdsState(offset, matchingPersistenceId, runCurrentPersistenceIdQuery))

}

class PersistenceIdsState(
    offset: AllEventsOffset,
    matchingPersistenceId: PersistenceIdsState.PersistenceId => Boolean,
    runCurrentPersistenceIdsQuery: ActorRef => Unit)
    extends Actor
    with Timers
    with ActorLogging {
  import PersistenceIdsState._

  private var pidState: Map[PersistenceId, PidState] = offset.offsets.iterator.map {
    case (pid, seqNr) => pid -> PidState(seqNr, Vector.empty, inFlight = false, lru = false)
  }.toMap
  // FIXME more efficient Queue?
  private var pending: Vector[PersistenceId] = pidState.keysIterator.toVector.sorted
  private var directPending: Vector[PersistenceId] = Vector.empty

  // delay every 100th query
  // FIXME real config
  private val lowThrottleDelay =
    context.system.settings.config.getDuration("all-events-query.low-throttle-delay").asScala
  private val highThrottleDelay =
    context.system.settings.config.getDuration("all-events-query.high-throttle-delay").asScala

  private var throttleDelay: FiniteDuration = lowThrottleDelay
  private var queryCount = 0
  private var updateCount = 0
  private var resetPendingTime = System.nanoTime()

  // FIXME real config
  private val currentPersistenceIdsQueryInitialDelay =
    context.system.settings.config.getDuration("all-events-query.scan-all-initial-delay").asScala
  private val currentPersistenceIdsQueryInterval =
    context.system.settings.config.getDuration("all-events-query.scan-all-interval").asScala
  private var currentPersistenceIdsQueryState: CurrentPersistenceIdsQueryState = CurrentPersistenceIdsQueryIdle

  context.actorOf(LruPersistenceIds.props(self, matchingPersistenceId), "lruPids")

  log.debug("Starting PersistenceIdsState [{}] with [{}] pids in offset.", self.path.name, offset.offsets.size)

  timers.startSingleTimer(
    CurrentPersistenceIdsQueryTick,
    CurrentPersistenceIdsQueryTick,
    currentPersistenceIdsQueryInitialDelay + ThreadLocalRandom
      .current()
      .nextInt(currentPersistenceIdsQueryInitialDelay.toSeconds.toInt)
      .seconds)

  private def scheduler = context.system.scheduler

  private def nextDirectPending(): Option[(PersistenceId, PidState)] = {
    if (directPending.isEmpty) {
      None
    } else {

      val iter = directPending.iterator
      @tailrec def findNext(): Option[(PersistenceId, PidState)] = {
        if (iter.isEmpty) {
          None
        } else {
          val pid = iter.next()
          val state = pidState(pid)
          if (state.inFlight || state.bufferedDirect.isEmpty) {
            findNext()
          } else {
            // FIXME filterNot is not very efficient, better mutable collection for this purpose?
            directPending = directPending.filterNot(_ == pid)
            Some(pid -> state)
          }
        }
      }

      findNext()
    }
  }

  private def nextPending(): Option[(PersistenceId, PidState)] = {
    if (pending.isEmpty && pidState.nonEmpty) {
      val durationMs = (System.nanoTime() - resetPendingTime).nanos.toMillis
      // FIXME debug level
      log.info(
        "Reset pending pids. Previous round took [{} ms]. Queried [{}] pids. Found events for [{}] pids.",
        durationMs,
        queryCount,
        updateCount)
      resetPendingTime = System.nanoTime()

      if (currentPersistenceIdsQueryState == UseAllPidsNextTime) {
        pending = pidState.keysIterator.toVector.sorted
        // delay every 100th query
        throttleDelay = lowThrottleDelay
        currentPersistenceIdsQueryState = CurrentPersistenceIdsQueryIdle
        timers.startSingleTimer(
          CurrentPersistenceIdsQueryTick,
          CurrentPersistenceIdsQueryTick,
          currentPersistenceIdsQueryInterval)
      } else {
        // use active pids
        pending = pidState
          .collect {
            case (pid, state) if state.lru => pid
          }
          .toVector
          .sorted
        if (updateCount == 0) {
          // delay every 100th query
          // measured 10000 pids: 6 seconds without throttling, 1 minute with 500 ms/100 throttling
          // measured 100000 pids: 70 seconds without throttling, 10 minutes with 500 ms/100 throttling
          throttleDelay = highThrottleDelay
        } else {
          throttleDelay = lowThrottleDelay
        }
      }

      updateCount = 0
      queryCount = 0
    }

    @tailrec def findNext(): Option[(PersistenceId, PidState)] = {
      if (pending.isEmpty) {
        None
      } else {
        val pid = pending.head
        pending = pending.tail

        val state = pidState(pid)
        if (state.inFlight || state.bufferedDirect.nonEmpty)
          findNext()
        else
          Some(pid -> state)
      }
    }
    findNext()
  }

  override def receive: Receive = {
    case direct: PersistentRepr =>
      val pid = direct.persistenceId
      if (matchingPersistenceId(pid)) {
        pidState.get(pid) match {
          case Some(s @ PidState(seqNr, buffered, _, _)) =>
            val expectedSeqNr =
              if (buffered.isEmpty) seqNr + 1
              else buffered.last.sequenceNr + 1
            if (direct.sequenceNr == expectedSeqNr) {
              log.debug("Received direct event pid [{}] seqNr [{}].", pid, direct.sequenceNr)
              pidState = pidState.updated(pid, s.copy(bufferedDirect = buffered :+ direct))
              directPending :+= pid
            } else {
              log.info(
                "Received direct event pid [{}] seqNr [{}], but expected seqNr [{}].",
                pid,
                direct.sequenceNr,
                seqNr + 1)
              // add first to give query for this pid priority
              pending = pid +: pending
            }
          case None =>
            if (direct.sequenceNr == 1L) {
              log.debug("Received direct event new pid [{}] seqNr [{}]", pid, direct.sequenceNr)
              pidState = pidState.updated(pid, PidState.empty.copy(bufferedDirect = Vector(direct)))
              directPending :+= pid
            } else {
              log.info(
                "Received direct event new pid [{}] seqNr [{}], but expected seqNr [{}].",
                pid,
                direct.sequenceNr,
                1)
              pidState = pidState.updated(pid, PidState.empty)
              // add first to give query for this pid priority
              pending = pid +: pending
            }
        }
      } else {
        log.info("Received direct event pid [{}] seqNr [{}], but not with matching pid", pid, direct.sequenceNr)
      }

    case GetNextPersistenceId =>
      // FIXME fairness between direct and other
      // first look for buffered direct events
      nextDirectPending() match {
        case Some((pid, state)) =>
          log.debug(
            "Next pid [{}] via direct event, seqNr [{} - {}].",
            pid,
            state.bufferedDirect.head.sequenceNr,
            state.bufferedDirect.last.sequenceNr)
          // also update the seqNr so that new direct events are accepted and buffered while these are in flight
          pidState = pidState.updated(
            pid,
            state.copy(seqNr = state.bufferedDirect.last.sequenceNr, bufferedDirect = Vector.empty, inFlight = true))
          sender() ! NextPersistenceId(pid, state.seqNr, state.bufferedDirect)

        case None =>
          // then look for next in the order of the `pending`
          nextPending() match {
            case Some((pid, state)) =>
              log.debug("Next pid [{}] from seqNr [{}].", pid, state.seqNr + 1)
              pidState = pidState.updated(pid, state.copy(inFlight = true))
              val reply = NextPersistenceId(pid, state.seqNr, Vector.empty)
              if (throttleDelay > Duration.Zero && queryCount % 100 == 0)
                scheduler.scheduleOnce(throttleDelay, sender(), reply)(context.dispatcher)
              else
                sender() ! reply
              queryCount += 1
            case None =>
              log.debug("No pending pids.")
              // slow down
              scheduler.scheduleOnce(highThrottleDelay, sender(), NextPersistenceId("", 0, Vector.empty))(
                context.dispatcher)
          }
      }

    case UpdatePersistenceIdOffset(pid, seqNr, directEvents) =>
      val state = pidState(pid)
      if (directEvents) {
        log.debug("Delivered direct events pid [{}] to seqNr [{}].", pid, seqNr)
      } else if (seqNr == state.seqNr) {
        log.debug("No events found pid [{}] from seqNr [{}].", pid, seqNr + 1)
      } else {
        throttleDelay = lowThrottleDelay
        updateCount += 1
        log.debug("Updated pid [{}] with seqNr [{}]. Found events for [{}] pids.", pid, seqNr, updateCount)
      }
      val newBufferredDirrect =
        if (state.bufferedDirect.isEmpty)
          state.bufferedDirect
        else
          state.bufferedDirect.dropWhile(_.sequenceNr <= seqNr)
      pidState =
        pidState.updated(pid, state.copy(seqNr = seqNr, bufferedDirect = newBufferredDirrect, inFlight = false))

    case LruPersistenceIds.ActivePids(pids) =>
      log.debug("Adding [{}] active pids.", pids.size)
      pids.foreach { pid =>
        pidState.get(pid) match {
          case Some(s) =>
            pidState = pidState.updated(pid, s.copy(lru = true))
          case None =>
            log.debug("New pid [{}].", pid)
            pidState = pidState.updated(pid, PidState.empty.copy(lru = true))
            // add first to give query for new pid priority
            pending = pid +: pending
        }
      }

    case LruPersistenceIds.InactivePids(pids) =>
      log.debug("Removing [{}] inactive pids.", pids.size)
      pids.foreach { pid =>
        pidState.get(pid) match {
          case Some(s) => pidState = pidState.updated(pid, s.copy(lru = false))
          case None    =>
        }
      }

    case CurrentPersistenceIdsQueryTick =>
      currentPersistenceIdsQueryState match {
        case CurrentPersistenceIdsQueryIdle =>
          currentPersistenceIdsQueryState = CurrentPersistenceIdsQueryInProgress
          log.debug("Run currentPersistenceIdsQuery")
          runCurrentPersistenceIdsQuery(self)
        case CurrentPersistenceIdsQueryInProgress | UseAllPidsNextTime =>
          timers.startSingleTimer(
            CurrentPersistenceIdsQueryTick,
            CurrentPersistenceIdsQueryTick,
            currentPersistenceIdsQueryInterval)
      }

    case PidsFromQuery(pids) =>
      // PersistenceId from currentPersistenceIds query
      pids.foreach { pid =>
        if (!pidState.contains(pid)) {
          log.debug("New pid from currentPersistenceIds query [{}].", pid)
          pidState = pidState.updated(pid, PidState.empty)
        }
      }
      sender() ! Done

    case PidsQueryCompleted =>
      currentPersistenceIdsQueryState = UseAllPidsNextTime
      log.debug("currentPersistenceIdsQuery completed")
  }
}
