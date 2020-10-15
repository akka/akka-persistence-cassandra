/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
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

        val persistenceIdsState: ActorRef = system.systemActorOf(
          PersistenceIdsState.props(offset, matchingPersistenceId),
          s"persistenceIdsState-${actorNameCounter.incrementAndGet()}")
        subscribeForDirectEvents(persistenceIdsState)

        val killSwitch = KillSwitches.shared(persistenceIdsState.path.name)

        // FIXME handle failures, restart
        // TODO we could maybe have a single ActorSystem global persistenceIds and merge with a currentPersistenceIds
        queries
          .persistenceIds()
          .via(killSwitch.flow)
          .filterNot(offset.offsets.contains) // already known
          .filter(matchingPersistenceId)
          .ask[Done](10)(persistenceIdsState)
          .runWith(Sink.ignore)(mat)

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

  private object PidState {
    val empty: PidState = PidState(0L, Vector.empty, inFlight = false)
  }
  private case class PidState(seqNr: SeqNr, bufferedDirect: Vector[PersistentRepr], inFlight: Boolean)

  def props(offset: AllEventsOffset, matchingPersistenceId: PersistenceIdsState.PersistenceId => Boolean): Props =
    Props(new PersistenceIdsState(offset, matchingPersistenceId))

}

class PersistenceIdsState(offset: AllEventsOffset, matchingPersistenceId: PersistenceIdsState.PersistenceId => Boolean)
    extends Actor
    with ActorLogging {
  import PersistenceIdsState._

  private var pidState: Map[PersistenceId, PidState] = offset.offsets.iterator.map {
    case (pid, seqNr) => pid -> PidState(seqNr, Vector.empty, inFlight = false)
  }.toMap
  // FIXME more efficient Queue?
  private var pending: Vector[PersistenceId] = pidState.keysIterator.toVector.sorted
  private var directPending: Vector[PersistenceId] = Vector.empty

  private var throttleDelay: FiniteDuration = Duration.Zero
  private var updateCount = 0

  log.debug("Starting PersistenceIdsState [{}] with [{}] pids in offset.", self.path.name, offset.offsets.size)

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
      log.debug("Reset pending pids, size [{}]. Found events for [{}] pids.", pidState.size, updateCount)
      if (updateCount == 0)
        throttleDelay = 50.millis // FIXME config
      updateCount = 0
      pending = pidState.keysIterator.toVector.sorted
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
    case pid: PersistenceId =>
      if (!pidState.contains(pid)) {
        log.debug("New pid [{}].", pid)
        pidState = pidState.updated(pid, PidState.empty)
        // add first to give new pid priority
        pending = pid +: pending
      }
      sender() ! Done

    case direct: PersistentRepr =>
      val pid = direct.persistenceId
      if (matchingPersistenceId(pid)) {
        pidState.get(pid) match {
          case Some(s @ PidState(seqNr, buffered, _)) =>
            val expectedSeqNr =
              if (buffered.isEmpty) seqNr + 1
              else buffered.last.sequenceNr + 1
            if (direct.sequenceNr == expectedSeqNr) {
              log.debug("Received direct event pid [{}] seqNr [{}].", pid, direct.sequenceNr)
              pidState = pidState.updated(pid, s.copy(bufferedDirect = buffered :+ direct))
              directPending :+= pid
            } else {
              log.debug(
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
              pidState = pidState.updated(pid, PidState(0L, Vector(direct), inFlight = false))
              directPending :+= pid
            } else {
              log.debug(
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
        log.debug("Received direct event pid [{}] seqNr [{}], but not with matching pid", pid, direct.sequenceNr)
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
              if (throttleDelay == Duration.Zero)
                sender() ! reply
              else
                scheduler.scheduleOnce(throttleDelay, sender(), reply)(context.dispatcher)
            case None =>
              log.debug("No pending pids.")
              // slow down
              scheduler.scheduleOnce(200.millis, sender(), NextPersistenceId("", 0, Vector.empty))(context.dispatcher)
          }
      }

    case UpdatePersistenceIdOffset(pid, seqNr, directEvents) =>
      val state = pidState(pid)
      if (directEvents) {
        log.debug("Delivered direct events pid [{}] to seqNr [{}].", pid, seqNr)
      } else if (seqNr == state.seqNr) {
        log.debug("No events found pid [{}] from seqNr [{}].", pid, seqNr + 1)
      } else {
        throttleDelay = Duration.Zero
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
  }
}
