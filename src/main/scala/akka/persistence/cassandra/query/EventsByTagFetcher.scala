/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Status
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.Bytes
import akka.actor.ActorLogging
import scala.annotation.tailrec
import akka.actor.DeadLetterSuppression
import akka.persistence.cassandra.journal.TimeBucket
import akka.actor.NoSerializationVerificationNeeded
import com.datastax.driver.core.Row
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra._

private[query] object EventsByTagFetcher {

  private final case class InitResultSet(rs: ResultSet)
    extends DeadLetterSuppression with NoSerializationVerificationNeeded
  private case object Fetched extends DeadLetterSuppression

  def props(tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int,
            backtracking: Boolean, replyTo: ActorRef,
            session: Session, preparedSelect: PreparedStatement, seqNumbers: Option[SequenceNumbers],
            settings: CassandraReadJournalConfig): Props =
    Props(new EventsByTagFetcher(tag, timeBucket, fromOffset, toOffset, limit, backtracking,
      replyTo, session, preparedSelect, seqNumbers, settings)).withDispatcher(settings.pluginDispatcher)

}

private[query] class EventsByTagFetcher(
  tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int, backtracking: Boolean,
  replyTo: ActorRef, session: Session, preparedSelect: PreparedStatement,
  seqN: Option[SequenceNumbers], settings: CassandraReadJournalConfig
)
  extends Actor with ActorLogging {

  import context.dispatcher
  import akka.persistence.cassandra.query.UUIDComparator.comparator.compare
  import EventsByTagFetcher._
  import EventsByTagPublisher._
  import CassandraJournal.deserializeEvent

  val serialization = SerializationExtension(context.system)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  var highestOffset: UUID = fromOffset
  var count = 0
  var seqNumbers = seqN

  override def preStart(): Unit = {
    val boundStmt = preparedSelect.bind(tag, timeBucket.key, fromOffset, toOffset, limit: Integer)
    boundStmt.setFetchSize(settings.fetchSize)
    session.executeAsync(boundStmt).asScala.map(InitResultSet.apply).pipeTo(self)
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
    if (resultSet.isExhausted) {
      replyTo ! ReplayDone(count, seqNumbers, highestOffset)
      context.stop(self)
    } else {

      @tailrec def loop(n: Int): Unit = {
        if (n == 0) {
          resultSet.fetchMoreResults.asScala.map(_ => Fetched).pipeTo(self)
        } else {
          count += 1
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
              replyTo ! UUIDPersistentRepr(offs, toPersistentRepr(row, pid, seqNr))
              loop(n - 1)

            case Some(s) =>
              s.isNext(pid, seqNr) match {
                case SequenceNumbers.Yes | SequenceNumbers.PossiblyFirst =>
                  seqNumbers = Some(s.updated(pid, seqNr))
                  replyTo ! UUIDPersistentRepr(offs, toPersistentRepr(row, pid, seqNr))
                  loop(n - 1)

                case SequenceNumbers.After =>
                  replyTo ! ReplayAborted(seqNumbers, pid, s.get(pid) + 1, seqNr)
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
