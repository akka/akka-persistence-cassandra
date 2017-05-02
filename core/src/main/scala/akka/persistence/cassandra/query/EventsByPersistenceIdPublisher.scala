/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import akka.actor.{ NoSerializationVerificationNeeded, Props }
import akka.persistence.PersistentRepr
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.query.EventsByPersistenceIdPublisher._
import akka.persistence.cassandra.query.QueryActorPublisher._
import akka.serialization.{ Serialization, SerializationExtension }
import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import com.datastax.driver.core.policies.RetryPolicy

private[query] object EventsByPersistenceIdPublisher {
  private[query] final case class EventsByPersistenceIdSession(
    selectEventsByPersistenceIdQuery: PreparedStatement,
    selectInUseQuery:                 PreparedStatement,
    selectDeletedToQuery:             PreparedStatement,
    session:                          Session,
    customConsistencyLevel:           Option[ConsistencyLevel],
    customRetryPolicy:                Option[RetryPolicy]
  ) extends NoSerializationVerificationNeeded {

    def selectEventsByPersistenceId(
      persistenceId: String,
      partitionNr:   Long,
      progress:      Long,
      toSeqNr:       Long,
      fetchSize:     Int
    )(implicit ec: ExecutionContext): Future[ResultSet] = {
      val boundStatement = selectEventsByPersistenceIdQuery.bind(persistenceId, partitionNr: JLong, progress: JLong, toSeqNr: JLong)
      boundStatement.setFetchSize(fetchSize)
      executeStatement(boundStatement)
    }

    def selectInUse(persistenceId: String, currentPnr: Long)(implicit ec: ExecutionContext): Future[ResultSet] =
      executeStatement(selectInUseQuery.bind(persistenceId, currentPnr: JLong))

    def selectDeletedTo(partitionKey: String)(implicit ec: ExecutionContext): Future[ResultSet] =
      executeStatement(selectDeletedToQuery.bind(partitionKey))

    private def executeStatement(statement: Statement)(implicit ec: ExecutionContext): Future[ResultSet] =
      listenableFutureToFuture(
        session.executeAsync(withCustom(statement))
      )

    private def withCustom(statement: Statement): Statement = {
      customConsistencyLevel.foreach(statement.setConsistencyLevel)
      customRetryPolicy.foreach(statement.setRetryPolicy)
      statement
    }
  }

  private[query] final case class EventsByPersistenceIdState(
    progress:    Long,
    count:       Long,
    partitionNr: Long
  )

  def props(
    persistenceId: String, fromSeqNr: Long, toSeqNr: Long, max: Long, fetchSize: Int,
    refreshInterval: Option[FiniteDuration], session: EventsByPersistenceIdSession,
    config: CassandraReadJournalConfig
  ): Props =
    Props(
      classOf[EventsByPersistenceIdPublisher], persistenceId, fromSeqNr, toSeqNr, max, fetchSize,
      refreshInterval, session, config
    )
}

private[query] class EventsByPersistenceIdPublisher(
  persistenceId: String, fromSeqNr: Long, toSeqNr: Long, max: Long, fetchSize: Int,
  refreshInterval: Option[FiniteDuration], session: EventsByPersistenceIdSession,
  config: CassandraReadJournalConfig
)
  extends QueryActorPublisher[PersistentRepr, EventsByPersistenceIdState](refreshInterval, config) {

  import CassandraJournal.deserializeEvent
  import context.dispatcher

  private[this] val serialization = SerializationExtension(context.system)

  override protected def initialQuery(initialState: EventsByPersistenceIdState): Future[Action] =
    query(initialState.copy(partitionNr = initialState.partitionNr - 1))

  override protected def completionCondition(state: EventsByPersistenceIdState): Boolean =
    state.progress > toSeqNr || state.count >= max

  override protected def initialState: Future[EventsByPersistenceIdState] =
    highestDeletedSequenceNumber(persistenceId).map { del =>
      val initialFromSequenceNr = math.max(del + 1, fromSeqNr)
      val currentPnr = partitionNr(initialFromSequenceNr, config.targetPartitionSize) + 1

      EventsByPersistenceIdState(initialFromSequenceNr, 0, currentPnr)
    }

  override protected def updateState(
    state: EventsByPersistenceIdState,
    row:   Row
  ): (Option[PersistentRepr], EventsByPersistenceIdState) = {
    val event = extractEvent(row)
    val partitionNr = row.getLong("partition_nr") + 1

    (Some(event), EventsByPersistenceIdState(event.sequenceNr + 1, state.count + 1, partitionNr))
  }

  private[this] def extractEvent(row: Row): PersistentRepr =
    row.getBytes("message") match {
      case null =>
        PersistentRepr(
          payload = deserializeEvent(serialization, row),
          sequenceNr = row.getLong("sequence_nr"),
          persistenceId = row.getString("persistence_id"),
          manifest = row.getString("event_manifest"),
          deleted = false,
          sender = null,
          writerUuid = row.getString("writer_uuid")
        )
      case b =>
        // for backwards compatibility
        persistentFromByteBuffer(serialization, b)
    }

  private[this] def persistentFromByteBuffer(
    serialization: Serialization,
    b:             ByteBuffer
  ): PersistentRepr =
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get

  override protected def requestNext(
    state:     EventsByPersistenceIdState,
    resultSet: ResultSet
  ): Future[Action] = {

    inUse(persistenceId, state.partitionNr).flatMap { i =>
      if (i) query(state)
      else Future.successful(Finished(resultSet))
    }
  }

  protected override def requestNextFinished(
    state:     EventsByPersistenceIdState,
    resultSet: ResultSet
  ): Future[Action] = {

    query(state.copy(partitionNr = state.partitionNr - 1))
      .flatMap { rs =>
        if (rs.rs.isExhausted) requestNext(state, resultSet)
        else Future.successful(rs)
      }
  }

  private[this] def inUse(persistenceId: String, currentPnr: Long): Future[Boolean] =
    session.selectInUse(persistenceId, currentPnr)
      .map(rs => if (rs.isExhausted) false else rs.one().getBool("used"))

  private[this] def highestDeletedSequenceNumber(partitionKey: String): Future[Long] =
    session.selectDeletedTo(partitionKey)
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))

  private[this] def partitionNr(sequenceNr: Long, targetPartitionSize: Int): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private[this] def query(state: EventsByPersistenceIdState): Future[NewResultSet] =
    session.selectEventsByPersistenceId(persistenceId, state.partitionNr, state.progress, toSeqNr, fetchSize).map(NewResultSet)
}
