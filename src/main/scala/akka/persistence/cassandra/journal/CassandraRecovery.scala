package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import scala.concurrent._

import com.datastax.driver.core.Row

import akka.persistence.PersistentRepr

trait CassandraRecovery { this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future { replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback) }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future { readHighestSequenceNr(persistenceId, fromSequenceNr ) }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    new MessageIterator(persistenceId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue).foldLeft(fromSequenceNr) { case (acc, msg) => msg.sequenceNr }

  def readLowestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    new MessageIterator(persistenceId, fromSequenceNr, Long.MaxValue, Long.MaxValue).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =
    new MessageIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach(replayCallback)

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {
    import PersistentRepr.Undefined

    private val iter = new RowIterator(persistenceId, fromSequenceNr, toSequenceNr)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    private var n: PersistentRepr = PersistentRepr(Undefined)

    fetch()

    def hasNext: Boolean =
      n != null && mcnt < max

    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      c
    }

    /**
     * Make next message n the current message c, complete c
     * (ignoring orphan markers) and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val marker = row.getString("marker")
        val snr = row.getLong("sequence_nr")
        if (marker == "A") {
          val m = persistentFromByteBuffer(row.getBytes("message"))
          // there may be duplicates returned by iter
          // (on scan boundaries within a partition)
          if (snr == c.sequenceNr) c = m else n = m
        } else if (marker == "B" && c.sequenceNr == snr) {
          c = c.update(deleted = true)
        }
      }
    }
  }

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    var currentPnr = partitionNr(fromSequenceNr)
    var currentSnr = fromSequenceNr

    var fromSnr = fromSequenceNr
    var toSnr = math.min(sequenceNrMax(currentPnr), toSequenceNr)

    var rcnt = 0
    var iter = newIter()

    def hasHeader = !session.execute(preparedSelectHeader.bind(persistenceId, currentPnr: JLong)).isExhausted
    def newIter() = session.execute(preparedSelectMessages.bind(persistenceId, currentPnr: JLong, fromSnr: JLong, toSnr: JLong)).iterator

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (iter.hasNext) {
        // more entries available in current partition
        true
      } else if (rcnt == 0 && !hasHeader) {
        // no more entries available, non-existing partition detected
        false
      } else if (rcnt < maxResultSize) {
        // all entries consumed, try next partition
        currentPnr += 1
        fromSnr = sequenceNrMin(currentPnr)
        toSnr = math.min(sequenceNrMax(currentPnr), toSequenceNr)
        rcnt = 0
        iter = newIter()
        hasNext
      } else {
        // max result set size reached, continue with same partition
        fromSnr = currentSnr
        rcnt = 0
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      currentSnr = row.getLong("sequence_nr")
      rcnt += 1
      row
    }

    private def sequenceNrMin(partitionNr: Long): Long =
      partitionNr * maxPartitionSize + 1L

    private def sequenceNrMax(partitionNr: Long): Long =
      (partitionNr + 1L) * maxPartitionSize
  }
}
