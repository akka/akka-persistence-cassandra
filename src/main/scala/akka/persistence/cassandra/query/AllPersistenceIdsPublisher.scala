package akka.persistence.cassandra.query

import scala.concurrent.duration.FiniteDuration

import akka.actor.Props
import com.datastax.driver.core.{ResultSet, Session, PreparedStatement}

import akka.persistence.cassandra.query.AllPersistenceIdsPublisher._

private[query] object AllPersistenceIdsPublisher {
  private[query] final case class AllPersistenceIdsSession(
    selectDistinctPersistenceIds: PreparedStatement,
    session: Session)
  private[query] final case class ReplayDone(resultSet: Option[ResultSet])
  private[query] final case class State(
    queryProgress: Option[ResultSet],
    knownPersistenceIds: Set[String],
    iteration: Int)

  def props(
      refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
      config: CassandraReadJournalConfig): Props =
    Props(new AllPersistenceIdsPublisher(refreshInterval, session, config))
}

private[query] class AllPersistenceIdsPublisher(
    refreshInterval: Option[FiniteDuration], session: AllPersistenceIdsSession,
    config: CassandraReadJournalConfig)
  extends QueryActorPublisher[String, State, String, ReplayDone](refreshInterval, config.maxBufferSize) {

  private[this] val step = config.maxBufferSize.toLong

  override protected def query(state: State, max: Long): Props =
    AllPersistenceIdsFetcher.props(self, session, Math.min(max, step), state.queryProgress, config)

  override protected def completionCondition(state: State): Boolean = false

  override protected def initialState: State = State(None, Set.empty, 0)

  override protected def updateBuffer(
      buf: Vector[String],
      newBuf: String,
      state: State): (Vector[String], State) = {
    if(state.knownPersistenceIds.contains(newBuf)) {
      (buf, state)
    } else {
      (buf :+ newBuf, state.copy(knownPersistenceIds = state.knownPersistenceIds + newBuf))
    }
  }
  
  /**
    * For currentPersistenceIds if dataset is exhausted we return the same state
    * (and the query publisher notices it did not change and stops). If dataset is not exhausted
    * we increment state. For allPersistenceIds however we need to restart state to None to start
    * scanning from beginning again (because order is not defined and new persistence ids may
    * appear anywhere in the result set).
    */
  override protected def updateState(
      state: State,
      replayDone: ReplayDone): State = {

    def nextIteration(state: State, resultSet: ResultSet): State =
      state.copy(queryProgress = Some(resultSet), iteration = state.iteration + 1)

    refreshInterval match {
      case Some(_) =>
        replayDone.resultSet
          .fold(state.copy(queryProgress = None))(nextIteration(state, _))
      case None =>
        replayDone.resultSet
          .fold(state)(nextIteration(state, _))
    }
  }
}
