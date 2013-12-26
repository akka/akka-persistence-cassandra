package akka.persistence.journal.cassandra

import java.lang.{ Long => JLong }

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util._

import akka.persistence.PersistentRepr

trait CassandraReplay { this: CassandraJournal =>
  private lazy val replayDispatcherId = config.getString("replay-dispatcher")
  private implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Future[Long] = {
    val promise = Promise[Long]
    session.executeAsync(preparedReplay.bind(processorId, fromSequenceNr: JLong, Long.MaxValue: JLong)) onComplete {
      case Failure(e) => promise.failure(e)
      case Success(r) =>
        if (r.isExhausted) promise.success(0L)
        else r.iterator.asScala.foreach { row =>
          r.getAvailableWithoutFetching
          val persistent = persistentFromByteBuffer(row.getBytes("message"))
          if (persistent.sequenceNr <= toSequenceNr) replayCallback(persistent)
          if (r.isExhausted) promise.success(persistent.sequenceNr)
        }
    }
    promise.future
  }
}
