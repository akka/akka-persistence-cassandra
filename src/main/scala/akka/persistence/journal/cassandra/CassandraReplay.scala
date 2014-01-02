package akka.persistence.journal.cassandra

import java.lang.{ Long => JLong }

import scala.collection.JavaConverters._
import scala.concurrent._

import akka.persistence.PersistentRepr

object CassandraReplay {
  case class ReplayState(rowCount: Int = 0, lastMessage: Option[PersistentRepr] = None) {
    def withMessage(message: PersistentRepr) =
      copy(rowCount = rowCount + 1, lastMessage = Some(message))

    def withConfirmation(channelId: String) =
      copy(rowCount = rowCount + 1, lastMessage = lastMessage.map(m => m.update(confirms = channelId +: m.confirms)))

    def withDeletion =
      copy(rowCount = rowCount + 1, lastMessage = lastMessage.map(_.update(deleted = true)))
  }
}

trait CassandraReplay { this: CassandraJournal =>
  import CassandraReplay._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString("replay-dispatcher"))

  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Future[Long] =
    Future { replay(processorId, fromSequenceNr, toSequenceNr)(replayCallback) }

  def replay(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Long = {
    var snr = fromSequenceNr
    var max = fromSequenceNr - 1L
    var pnr = partitionNr(fromSequenceNr)
    var valid = true

    while (valid) {
      replayPartition(processorId, pnr, snr, toSequenceNr)(replayCallback) match {
        case None => valid = false
        case Some(m) =>
          snr = sequenceNrMin(pnr)
          pnr = pnr + 1
          max = m
      }
    }
    max
  }

  @annotation.tailrec
  final def replayPartition(processorId: String, partitionNr: Long, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Option[Long] = {
    def replayOne(msg: PersistentRepr) = if (msg.sequenceNr <= toSequenceNr) replayCallback(msg)

    val rs = session.execute(preparedSelectMessages.bind(processorId, partitionNr: JLong, fromSequenceNr: JLong, sequenceNrMax(partitionNr): JLong))
    val state = rs.iterator.asScala.foldLeft(ReplayState()) {
      case (state, row) =>
        val marker = row.getString("marker")
        //
        // TODO: tolerate orphan markers
        //
        if (marker == "A") {
          state.lastMessage.foreach(replayOne)
          state.withMessage(persistentFromByteBuffer(row.getBytes("message")))
        } else if (marker == "B") {
          state.withDeletion
        } else {
          val channelId = marker.substring(2)
          state.withConfirmation(channelId)
        }
    }
    if (state.rowCount > 0) state.lastMessage match {
      case Some(msg) if (state.rowCount < maxResultSize) => replayOne(msg); Some(msg.sequenceNr)
      case Some(msg) => replayPartition(processorId, partitionNr, msg.sequenceNr, toSequenceNr)(replayCallback)
      case None => None
    } else {
      val rs = session.execute(preparedSelectHeader.bind(processorId, partitionNr: JLong))
      if (rs.isExhausted) None else Some(fromSequenceNr - 1L)
    }
  }

  private def sequenceNrMin(partitionNr: Long): Long =
    partitionNr * maxPartitionSize + 1L

  private def sequenceNrMax(partitionNr: Long): Long =
    (partitionNr + 1L) * maxPartitionSize
}
