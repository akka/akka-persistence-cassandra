package akka.persistence.journal.cassandra

import java.lang.{ Long => JLong }

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util._

import akka.persistence.PersistentRepr

import com.datastax.driver.core.ResultSet

trait CassandraReplay { this: CassandraJournal =>
  private lazy val replayDispatcherId = config.getString("replay-dispatcher")
  private implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Future[Long] = {
    val promise = Promise[Long]
    session.executeAsync(preparedReplay.bind(processorId, fromSequenceNr: JLong, Long.MaxValue: JLong)) onComplete {
      case Failure(e) => promise.failure(e)
      case Success(r) => promise.success(replay(r, toSequenceNr)(replayCallback))
    }
    promise.future
  }

  def replay(results: ResultSet, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Long = {
    var msg: PersistentRepr = null
    var snr = 0L

    def replayOne(msg: PersistentRepr) =
      if (msg != null) replayCallback(msg)

    results.iterator.asScala.foreach { row =>
      val marker = row.getString("marker")
      if (marker == "A") {
        replayOne(msg)
        msg = persistentFromByteBuffer(row.getBytes("message"))
        snr = msg.sequenceNr
      } else if (marker == "B") {
        msg = msg.update(deleted = true)
      } else {
        val channelId = marker.substring(2)
        msg = msg.update(confirms = channelId +: msg.confirms)
      }
    }
    replayOne(msg)
    snr
  }
}
