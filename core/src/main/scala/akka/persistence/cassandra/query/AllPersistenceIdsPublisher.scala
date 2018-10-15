/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import akka.actor.Props
import com.datastax.driver.core.{ Row, ResultSet, Session, PreparedStatement }
import akka.persistence.cassandra._
import akka.persistence.cassandra.query.AllPersistenceIdsPublisher._
import akka.persistence.cassandra.query.QueryActorPublisher._
import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AllPersistenceIdsPublisher {
  final case class AllPersistenceIdsSession(
    selectDistinctPersistenceIds: PreparedStatement,
    session:                      Session) extends NoSerializationVerificationNeeded
  final case class ReplayDone(resultSet: Option[ResultSet])
    extends NoSerializationVerificationNeeded
  final case class AllPersistenceIdsState(knownPersistenceIds: Set[String])

  def props(
    refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
    config: CassandraReadJournalConfig): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, session, config))
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class AllPersistenceIdsPublisher(
  refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
  config: CassandraReadJournalConfig)
  extends QueryActorPublisher[String, AllPersistenceIdsState](refreshInterval, config) {

  import context.dispatcher

  override protected def initialState: Future[AllPersistenceIdsState] =
    Future.successful(AllPersistenceIdsState(Set.empty))

  override protected def initialQuery(initialState: AllPersistenceIdsState): Future[Action] =
    query(initialState)

  override protected def completionCondition(state: AllPersistenceIdsState): Boolean = false

  override protected def updateState(
    state: AllPersistenceIdsState, row: Row): (Option[String], AllPersistenceIdsState) = {

    val event = row.getString("persistence_id")

    if (state.knownPersistenceIds.contains(event)) {
      (None, state)
    } else {
      (Some(event), state.copy(knownPersistenceIds = state.knownPersistenceIds + event))
    }
  }

  override protected def requestNext(
    state:     AllPersistenceIdsState,
    resultSet: ResultSet): Future[Action] =
    query(state)

  override protected def requestNextFinished(
    state:     AllPersistenceIdsState,
    resultSet: ResultSet): Future[Action] =
    requestNext(state, resultSet)

  private[this] def query(state: AllPersistenceIdsState): Future[Action] = {
    val boundStatement = session.selectDistinctPersistenceIds.bind()
    boundStatement.setFetchSize(config.fetchSize)

    listenableFutureToFuture(session.session.executeAsync(boundStatement)).map(Finished)
  }
}
