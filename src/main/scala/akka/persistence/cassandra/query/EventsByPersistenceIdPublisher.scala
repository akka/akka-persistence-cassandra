/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.lang.{ Long => JLong }
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.serialization.{ SerializationExtension, Serialization }
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ ResultSet, Row, PreparedStatement, Session }
import akka.persistence.cassandra._
import akka.persistence.cassandra.query.QueryActorPublisher._
import akka.persistence.cassandra.query.EventsByPersistenceIdPublisher._
import akka.persistence.cassandra.journal.CassandraJournal

private[query] object EventsByPersistenceIdPublisher {
  private[query] final case class EventsByPersistenceIdSession(
    selectEventsByPersistenceId: PreparedStatement,
    selectInUse:                 PreparedStatement,
    selectDeletedTo:             PreparedStatement,
    session:                     Session
  )

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
      new EventsByPersistenceIdPublisher(persistenceId, fromSeqNr, toSeqNr, max, fetchSize,
        refreshInterval, session, config)
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
    highestDeletedSequenceNumber(persistenceId, session.selectDeletedTo).map { del =>
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

  private[this] def inUse(persistenceId: String, currentPnr: Long): Future[Boolean] = {
    session.session
      .executeAsync(session.selectInUse.bind(persistenceId, currentPnr: JLong))
      .map(rs => if (rs.isExhausted) false else rs.one().getBool("used"))
  }

  private[this] def highestDeletedSequenceNumber(
    partitionKey:            String,
    preparedSelectDeletedTo: PreparedStatement
  ): Future[Long] = {
    listenableFutureToFuture(
      session.session.executeAsync(preparedSelectDeletedTo.bind(partitionKey))
    )
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }

  private[this] def partitionNr(sequenceNr: Long, targetPartitionSize: Int): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private[this] def query(state: EventsByPersistenceIdState): Future[NewResultSet] = {
    val boundStatement =
      session.selectEventsByPersistenceId.bind(
        persistenceId, state.partitionNr: JLong, state.progress: JLong, toSeqNr: JLong
      )

    boundStatement.setFetchSize(fetchSize)

    /*val st = session.session.getState
    val hosts = st.getConnectedHosts.asScala
    val cons = st.getOpenConnections(hosts.head)
    val inflight = st.getInFlightQueries(hosts.head)

    println(s"Host ${hosts.head}, connections $cons, open $inflight")*/

    listenableFutureToFuture(session.session.executeAsync(boundStatement)).map(NewResultSet)
  }
}
