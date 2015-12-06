package akka.persistence.cassandra.query

import scala.concurrent.duration.FiniteDuration

import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.persistence.query.EventEnvelope
import com.datastax.driver.core.{PreparedStatement, Session}

import akka.persistence.cassandra.query.EventsByPersistenceIdPublisher._

private[query] object EventsByPersistenceIdPublisher {
  private[query] final case class EventsByPersistenceIdSession(
    selectEventsByPersistenceId: PreparedStatement,
    selectInUse: PreparedStatement,
    selectDeletedTo: PreparedStatement,
    session: Session)
  private[query] final case class ReplayDone()

  def props(
      persistenceId: String, fromSeqNr: Long, toSeqNr: Long, refreshInterval: Option[FiniteDuration],
      session: EventsByPersistenceIdSession, config: CassandraReadJournalConfig): Props =
    Props(
      new EventsByPersistenceIdPublisher(persistenceId, fromSeqNr, toSeqNr, refreshInterval,
        session, config))
}

private[query] class EventsByPersistenceIdPublisher(
    persistenceId: String, fromSeqNr: Long, toSeqNr: Long, refreshInterval: Option[FiniteDuration],
    session: EventsByPersistenceIdSession, config: CassandraReadJournalConfig)
  extends QueryActorPublisher[EventEnvelope, Long, PersistentRepr, ReplayDone](refreshInterval, config.maxBufferSize) {

  private[this] val step = config.maxBufferSize

  override protected def query(state: Long, max: Long): Props = {
    val to = Math.min(Math.min(state + step, toSeqNr), state + max)

    EventsByPersistenceIdFetcher.props(persistenceId, state, to, self, session, config)
  }

  override protected def initialState: Long = Math.max(1, fromSeqNr)

  override def updateBuffer(
      buffer: Vector[EventEnvelope],
      newBuffer: PersistentRepr,
      state: Long): (Vector[EventEnvelope], Long) = {

    val addToBuffer = toEventEnvelope(newBuffer, newBuffer.sequenceNr)
    val newState = newBuffer.sequenceNr + 1

    (buffer :+ addToBuffer, newState)
  }

  override protected def completionCondition(state: Long): Boolean = state > toSeqNr

  private[this] def toEventEnvelope(persistentRepr: PersistentRepr, offset: Long): EventEnvelope =
    EventEnvelope(offset, persistentRepr.persistenceId, persistentRepr.sequenceNr, persistentRepr.payload)

  override protected def updateState(state: Long, replayDone: ReplayDone): Long = state
}
