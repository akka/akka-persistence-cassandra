package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import scala.concurrent._

import akka.actor.ActorLogging
import akka.persistence.PersistentRepr
import com.datastax.driver.core.{ ResultSet, Row }

trait CassandraRecovery extends ActorLogging {
  this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = Future {
    replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback)
  }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = Future {
    readHighestSequenceNr(persistenceId, fromSequenceNr)
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    findHighestSequenceNr(persistenceId, math.max(fromSequenceNr, highestDeletedSequenceNumber(persistenceId)))
  }

  def readLowestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    new MessageIterator(persistenceId, fromSequenceNr, Long.MaxValue, Long.MaxValue).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit = {
    new MessageIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach(msg => {
      replayCallback(msg)
    })
  }

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

    import PersistentRepr.Undefined

    private val initialFromSequenceNr = math.max(highestDeletedSequenceNumber(persistenceId) + 1, fromSequenceNr)
    log.debug("Starting message scan from {}", initialFromSequenceNr)

    private val iter = new RowIterator(persistenceId, initialFromSequenceNr, toSequenceNr)
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
     * and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val snr = row.getLong("sequence_nr")
        val m = persistentFromByteBuffer(row.getBytes("message"))
        // there may be duplicates returned by iter
        // (on scan boundaries within a partition)
        if (snr == c.sequenceNr) c = m else n = m
      }
    }
  }

  private def findHighestSequenceNr(persistenceId: String, fromSequenceNr: Long) = {
    import cassandraSession._
    @annotation.tailrec
    def find(currentPnr: Long, currentSnr: Long): Long = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val next = Option(session.execute(preparedSelectHighestSequenceNr.bind(persistenceId, currentPnr: JLong)).one())
        .map(row => (row.getBool("used"), row.getLong("sequence_nr")))
      next match {
        // never been to this partition
        case None                   => currentSnr
        // don't currently explicitly set false
        case Some((false, _))       => currentSnr
        // everything deleted in this partition, move to the next
        case Some((true, 0))        => find(currentPnr + 1, currentSnr)
        case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
      }
    }
    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  private def highestDeletedSequenceNumber(persistenceId: String): Long = {
    Option(session.execute(cassandraSession.preparedSelectDeletedTo.bind(persistenceId)).one())
      .map(_.getLong("deleted_to")).getOrElse(0)
  }

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    import cassandraSession._
    var currentPnr = partitionNr(fromSequenceNr)
    var currentSnr = fromSequenceNr

    var fromSnr = fromSequenceNr
    var toSnr = toSequenceNr

    var iter = newIter()

    def newIter() = {
      session.execute(preparedSelectMessages.bind(persistenceId, currentPnr: JLong, fromSnr: JLong, toSnr: JLong)).iterator
    }

    def inUse: Boolean = {
      val execute: ResultSet = session.execute(preparedCheckInUse.bind(persistenceId, currentPnr: JLong))
      if (execute.isExhausted) false
      else execute.one().getBool("used")
    }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (iter.hasNext) {
        // more entries available in current resultset
        true
      } else if (!inUse) {
        // partition has never been in use so stop
        false
      } else {
        // all entries consumed, try next partition
        currentPnr += 1
        fromSnr = currentSnr
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      currentSnr = row.getLong("sequence_nr")
      row
    }

    private def sequenceNrMin(partitionNr: Long): Long =
      partitionNr * targetPartitionSize + 1L

    private def sequenceNrMax(partitionNr: Long): Long =
      (partitionNr + 1L) * targetPartitionSize
  }
}
