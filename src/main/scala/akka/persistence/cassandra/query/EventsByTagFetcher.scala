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
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.Bytes
import akka.actor.ActorLogging
import scala.annotation.tailrec
import akka.actor.DeadLetterSuppression
import akka.persistence.cassandra.journal.TimeBucket

private[query] object EventsByTagFetcher {

  private final case class InitResultSet(rs: ResultSet) extends DeadLetterSuppression
  private case object Fetched extends DeadLetterSuppression

  def props(tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int,
            backtracking: Boolean, replyTo: ActorRef,
            session: Session, preparedSelect: PreparedStatement, seqNumbers: SequenceNumbers,
            settings: CassandraReadJournalConfig): Props =
    Props(new EventsByTagFetcher(tag, timeBucket, fromOffset, toOffset, limit, backtracking,
      replyTo, session, preparedSelect, seqNumbers, settings)).withDispatcher(settings.pluginDispatcher)

}

private[query] class EventsByTagFetcher(
  tag: String, timeBucket: TimeBucket, fromOffset: UUID, toOffset: UUID, limit: Int, backtracking: Boolean,
  replyTo: ActorRef, session: Session, preparedSelect: PreparedStatement,
  seqN: SequenceNumbers, settings: CassandraReadJournalConfig
)
  extends Actor with ActorLogging {

  import context.dispatcher
  import akka.persistence.cassandra.listenableFutureToFuture
  import akka.persistence.cassandra.query.UUIDComparator.comparator.compare
  import EventsByTagFetcher._
  import EventsByTagPublisher._

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
    val init: Future[ResultSet] = session.executeAsync(boundStmt)
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
      replyTo ! ReplayDone(count, seqNumbers, highestOffset)
      context.stop(self)
    } else {

      @tailrec def loop(n: Int): Unit = {
        if (n == 0) {
          val more: Future[ResultSet] = resultSet.fetchMoreResults()
          more.map(_ => Fetched).pipeTo(self)
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

          seqNumbers.isNext(pid, seqNr) match {
            case SequenceNumbers.Yes | SequenceNumbers.PossiblyFirst =>
              seqNumbers = seqNumbers.updated(pid, seqNr)
              val m = persistentFromByteBuffer(row.getBytes("message"))
              val eventEnvelope = UUIDEventEnvelope(
                offset = offs,
                persistenceId = pid,
                sequenceNr = row.getLong("sequence_nr"),
                event = m.payload
              )
              replyTo ! eventEnvelope
              loop(n - 1)

            case SequenceNumbers.After =>
              replyTo ! ReplayAborted(seqNumbers, pid, seqNumbers.get(pid) + 1, seqNr)
            // end loop

            case SequenceNumbers.Before =>
              // duplicate, discard
              if (!backtracking)
                log.debug(s"Discarding duplicate. Got sequence number [$seqNr] for [$pid], " +
                  s"but current sequence number is [${seqNumbers.get(pid)}]")
              loop(n - 1)
          }
        }
      }

      loop(resultSet.getAvailableWithoutFetching)

    }
  }

}
