/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import akka.actor._
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request, SubscriptionTimeoutExceeded }
import com.datastax.driver.core.{ ResultSet, Row }
import akka.persistence.cassandra._
import akka.persistence.cassandra.query.QueryActorPublisher._

import scala.util.control.NonFatal
import akka.annotation.InternalApi
import java.util.concurrent.ThreadLocalRandom

/**
 * INTERNAL API
 */
@InternalApi private[akka] object QueryActorPublisher {
  final case class ReplayFailed(cause: Throwable)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded

  sealed trait Action extends DeadLetterSuppression with NoSerializationVerificationNeeded
  final case class NewResultSet(rs: ResultSet) extends Action
  final case class FetchedResultSet(rs: ResultSet) extends Action
  final case class Finished(resultSet: ResultSet) extends Action

  case object Continue extends DeadLetterSuppression
}

//TODO: Handle database timeout, retry and failure handling.
//TODO: Write tests for buffer size, delivery buffer etc.
/**
 * INTERNAL API
 *
 * Abstract Query publisher. Can be integrated with Akka Streams as a Source.
 * Intended to be extended by concrete Query publisher classes. This class manages the stream
 * lifecycle, live stream updates, refreshInterval, max buffer size and causal consistency given an
 * offset queryable data source.
 *
 * @param refreshInterval Refresh interval.
 * @param config Configuration.
 * @tparam MessageType Type of message.
 * @tparam State Type of state.
 */
@InternalApi private[akka] abstract class QueryActorPublisher[MessageType, State: ClassTag](
  refreshInterval: Option[FiniteDuration],
  config:          CassandraReadJournalConfig)
  extends ActorPublisher[MessageType]
  with ActorLogging {

  private[this] sealed trait InitialAction extends NoSerializationVerificationNeeded
  private[this] case class InitialNewResultSet(s: State, rs: ResultSet) extends InitialAction
  private[this] case class InitialFinished(s: State, rs: ResultSet) extends InitialAction

  import context.dispatcher

  private[this] val tickTask =
    refreshInterval.map { i =>
      val initial =
        if (i >= 2.seconds) ThreadLocalRandom.current().nextLong(i.toMillis).millis
        else i
      context.system.scheduler.schedule(initial, i, self, Continue)(context.dispatcher)
    }

  override def postStop(): Unit = {
    tickTask.foreach(_.cancel())
    super.postStop()
  }

  override def preStart(): Unit = initialState
    .flatMap { state =>
      initialQuery(state).map {
        case NewResultSet(rs)     => InitialNewResultSet(state, rs)
        case FetchedResultSet(rs) => InitialNewResultSet(state, rs)
        case Finished(rs)         => InitialFinished(state, rs)
      }
    }
    .pipeTo(self)

  private[this] val starting: Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case InitialNewResultSet(s, newRs) =>
      context.become(exhaustFetchAndBecome(newRs, s, finished = false, continue = false))
    case InitialFinished(s, newRs) =>
      context.become(exhaustFetchAndBecome(newRs, s, finished = true, continue = false))
    case Status.Failure(cause) =>
      onErrorThenStop(cause)
  }

  private[this] def awaiting(rs: ResultSet, s: State, finished: Boolean): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case Request(_) =>
      context.become(exhaustFetchAndBecome(rs, s, finished, continue = false, Some(awaiting)))
    case NewResultSet(newRs) =>
      context.become(exhaustFetchAndBecome(newRs, s, finished, continue = false))
    case FetchedResultSet(newRs) =>
      context.become(exhaustFetchAndBecome(newRs, s, finished, continue = false))
    case Finished(newRs) =>
      context.become(exhaustFetchAndBecome(newRs, s, finished = true, continue = false))
    case Status.Failure(cause) =>
      onErrorThenStop(cause)
  }

  private[this] def idle(rs: ResultSet, s: State, f: Boolean): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case Request(_) =>
      context.become(exhaustFetchAndBecome(rs, s, f, false))
    case Continue =>
      context.become(exhaustFetchAndBecome(rs, s, f, true))
    case Status.Failure(cause) =>
      onErrorThenStop(cause)
  }

  override def receive: Receive = starting

  private[this] def exhaustFetchAndBecome(
    resultSet: ResultSet,
    state:     State,
    finished:  Boolean,
    continue:  Boolean,
    behaviour: Option[(ResultSet, State, Boolean) => Receive] = None): Receive = {

    try {
      val (newRs, newState) =
        exhaustFetch(resultSet, state, resultSet.getAvailableWithoutFetching, 0, totalDemand)

      behaviour match {
        case None    => nextBehavior(newRs, newState, finished, continue)
        case Some(b) => b(newRs, newState, finished)
      }
    } catch {
      case NonFatal(t) =>
        onErrorThenStop(t)
        Actor.ignoringBehavior
    }
  }

  // TODO: Optimize.
  private[this] def nextBehavior(
    resultSet: ResultSet,
    state:     State,
    finished:  Boolean,
    continue:  Boolean): Receive = {

    val availableWithoutFetching = resultSet.getAvailableWithoutFetching
    val isFullyFetched = resultSet.isFullyFetched

    if (shouldFetchMore(
      availableWithoutFetching, isFullyFetched, totalDemand, state, finished, continue)) {
      listenableFutureToFuture(resultSet.fetchMoreResults())
        .map(FetchedResultSet)
        .pipeTo(self)
      awaiting(resultSet, state, finished)
    } else if (shouldIdle(availableWithoutFetching, state)) {
      idle(resultSet, state, finished)
    } else {
      val exhausted = isExhausted(resultSet)

      if (shouldComplete(exhausted, refreshInterval, state, finished)) {
        onCompleteThenStop()
        Actor.emptyBehavior
      } else if (shouldRequestMore(exhausted, totalDemand, state, finished, continue)) {
        if (finished) requestNextFinished(state, resultSet).pipeTo(self)
        else requestNext(state, resultSet).pipeTo(self)
        awaiting(resultSet, state, finished)
      } else {
        idle(resultSet, state, finished)
      }
    }
  }

  private[this] def shouldIdle(availableWithoutFetching: Int, state: State) =
    availableWithoutFetching > 0 && !completionCondition(state)

  private[this] def shouldFetchMore(
    availableWithoutFetching: Int,
    isFullyFetched:           Boolean,
    demand:                   Long,
    state:                    State,
    finished:                 Boolean,
    continue:                 Boolean) =
    !isFullyFetched &&
      (availableWithoutFetching + config.fetchSize <= config.maxBufferSize
        || availableWithoutFetching == 0) &&
        !completionCondition(state)

  private[this] def shouldRequestMore(
    isExhausted: Boolean,
    demand:      Long,
    state:       State,
    finished:    Boolean,
    continue:    Boolean) =
    (!completionCondition(state) || refreshInterval.isDefined) &&
      !(finished && !continue) &&
      isExhausted

  private[this] def shouldComplete(
    isExhausted:     Boolean,
    refreshInterval: Option[FiniteDuration],
    state:           State,
    finished:        Boolean) =
    (finished && refreshInterval.isEmpty && isExhausted) || completionCondition(state)

  // ResultSet methods isExhausted(), one() etc. cause blocking database fetch if there aren't
  // any available items in the ResultSet buffer and it is not the last fetch batch
  // so we need to avoid calling them unless we know it won't block. See e.g. ArrayBackedResultSet.
  private[this] def isExhausted(resultSet: ResultSet): Boolean = resultSet.isExhausted

  @tailrec
  final protected def exhaustFetch(
    resultSet: ResultSet,
    state:     State,
    available: Int,
    count:     Long,
    max:       Long): (ResultSet, State) = {
    if (available == 0 || count == max || completionCondition(state)) {
      (resultSet, state)
    } else {
      val (event, nextState) = updateState(state, resultSet.one())
      event match {
        case Some(evt) => onNext(evt)
        case None      =>
      }
      exhaustFetch(resultSet, nextState, available - 1, count + 1, max)
    }
  }

  protected def initialState: Future[State]
  protected def initialQuery(initialState: State): Future[Action]
  protected def requestNext(state: State, resultSet: ResultSet): Future[Action]
  protected def requestNextFinished(state: State, resultSet: ResultSet): Future[Action]
  protected def updateState(state: State, row: Row): (Option[MessageType], State)
  protected def completionCondition(state: State): Boolean
}
