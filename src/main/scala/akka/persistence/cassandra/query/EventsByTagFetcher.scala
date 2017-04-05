/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.util.UUID

import scala.concurrent.Future
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Status
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.utils.Bytes
import akka.actor.ActorLogging
import scala.annotation.tailrec
import akka.actor.DeadLetterSuppression
import akka.persistence.cassandra.journal.TimeBucket
import akka.actor.NoSerializationVerificationNeeded
import akka.persistence.cassandra.PreparedStatementEnvelope
import com.datastax.driver.core.Row
import akka.persistence.cassandra.journal.CassandraJournal

private[query] object EventsByTagFetcher {

  private final case class InitResultSet(rs: ResultSet)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded
  private case object Fetched extends DeadLetterSuppression

  def props(tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int,
            backtracking: Boolean, replyTo: ActorRef,
            preparedSelect: PreparedStatementEnvelope, seqNumbers: Option[SequenceNumbers],
            settings: CassandraReadJournalConfig): Props =
    Props(classOf[EventsByTagFetcher], tag, timeBucket, fromOffset, toOffset, limit, backtracking,
      replyTo, preparedSelect, seqNumbers, settings).withDispatcher(settings.pluginDispatcher)

}

private[query] class EventsByTagFetcher(
  tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int, backtracking: Boolean,
  replyTo: ActorRef, preparedSelect: PreparedStatementEnvelope,
  seqN: Option[SequenceNumbers], settings: CassandraReadJournalConfig
)
  extends Actor with ActorLogging {

  import context.dispatcher
  import akka.persistence.cassandra.listenableFutureToFuture
  import akka.persistence.cassandra.query.UUIDComparator.comparator.compare
  import EventsByTagFetcher._
  import EventsByTagPublisher._
  import CassandraJournal.deserializeEvent

  val serialization = SerializationExtension(context.system)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  var highestOffset: UUID = fromOffset
  var retrievedCount = 0
  var deliveredCount = 0
  var seqNumbers = seqN

  override def preStart(): Unit = {
    val boundStmt = preparedSelect.ps.bind(tag, timeBucket.key, fromOffset, toOffset, limit: Integer)
    boundStmt.setFetchSize(settings.fetchSize)
    val init: Future[ResultSet] = preparedSelect.session.executeAsync(boundStmt)
    init.map(InitResultSet.apply).pipeTo(self)
  }

  def receive = {
    case InitResultSet(rs) =>
      context.become(active(rs))
      continue(rs)
    case Status.Failure(e) =>
      // from pipeTo
      throw e
  }

  def active(resultSet: ResultSet): Receive = {
    case Fetched =>
      continue(resultSet)
    case Status.Failure(e) =>
      // from pipeTo
      throw e
  }

  def continue(resultSet: ResultSet): Unit = {
    if (resultSet.isExhausted()) {
      replyTo ! ReplayDone(retrievedCount, deliveredCount, seqNumbers, highestOffset)
      context.stop(self)
    } else {

      @tailrec def loop(n: Int): Unit = {
        if (n == 0) {
          val more: Future[ResultSet] = resultSet.fetchMoreResults()
          more.map(_ => Fetched).pipeTo(self)
        } else {
          retrievedCount += 1
          val row = resultSet.one()
          val pid = row.getString("persistence_id")
          val seqNr = row.getLong("sequence_nr")

          val offs = row.getUUID("timestamp")
          if (compare(offs, highestOffset) <= 0)
            log.debug("Events were not ordered by timestamp. Consider increasing eventual-consistency-delay " +
              "if the order is of importance.")
          else
            highestOffset = offs

          seqNumbers match {
            case None =>
              deliveredCount += 1
              replyTo ! UUIDPersistentRepr(offs, toPersistentRepr(row, pid, seqNr))
              loop(n - 1)

            case Some(s) =>
              s.isNext(pid, seqNr) match {
                case SequenceNumbers.Yes =>
                  seqNumbers = Some(s.updated(pid, seqNr))
                  deliveredCount += 1
                  replyTo ! UUIDPersistentRepr(offs, toPersistentRepr(row, pid, seqNr))
                  loop(n - 1)

                case SequenceNumbers.PossiblyFirst =>
                  if (backtracking) {
                    // when in backtracking mode we use the seen sequence number as the starting point
                    seqNumbers = Some(s.updated(pid, seqNr))
                    deliveredCount += 1
                    replyTo ! UUIDPersistentRepr(offs, toPersistentRepr(row, pid, seqNr))
                    loop(n - 1)
                  } else {
                    // otherwise we need to go back and try to find earlier event
                    replyTo ! ReplayAborted(seqNumbers, pid, expectedSeqNr = None, gotSeqNr = seqNr)
                    // end loop
                  }

                case SequenceNumbers.After =>
                  replyTo ! ReplayAborted(seqNumbers, pid, expectedSeqNr = Some(s.get(pid) + 1), gotSeqNr = seqNr)
                // end loop

                case SequenceNumbers.Before =>
                  // duplicate, discard
                  if (!backtracking)
                    log.debug(s"Discarding duplicate. Got sequence number [$seqNr] for [$pid], " +
                      s"but current sequence number is [${s.get(pid)}]")
                  loop(n - 1)
              }
          }
        }
      }

      loop(resultSet.getAvailableWithoutFetching)

    }
  }

  private def toPersistentRepr(row: Row, persistenceId: String, sequenceNr: Long): PersistentRepr =
    row.getBytes("message") match {
      case null =>
        PersistentRepr(
          payload = deserializeEvent(serialization, row),
          sequenceNr = sequenceNr,
          persistenceId = persistenceId,
          manifest = row.getString("event_manifest"),
          deleted = false,
          sender = null,
          writerUuid = row.getString("writer_uuid")
        )
      case b =>
        // for backwards compatibility
        persistentFromByteBuffer(b)
    }

}
