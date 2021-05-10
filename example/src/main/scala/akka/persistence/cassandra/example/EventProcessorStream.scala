package akka.persistence.cassandra.example

import akka.{ Done, NotUsed }
import akka.actor.typed.ActorSystem
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ Offset, PersistenceQuery, TimeBasedUUID }
import akka.persistence.typed.PersistenceId
import akka.stream.SharedKillSwitch
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.{ RestartSource, Sink, Source }
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row }
import org.slf4j.{ Logger, LoggerFactory }
import akka.actor.typed.scaladsl.LoggerOps
import org.HdrHistogram.Histogram

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.reflect.ClassTag

class EventProcessorStream[Event: ClassTag](
    system: ActorSystem[_],
    executionContext: ExecutionContext,
    eventProcessorId: String,
    tag: String) {

  protected val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val sys: ActorSystem[_] = system
  implicit val ec: ExecutionContext = executionContext

  private val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")

  private val query = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def runQueryStream(killSwitch: SharedKillSwitch, histogram: Histogram): Unit = {
    RestartSource
      .withBackoff(minBackoff = 500.millis, maxBackoff = 20.seconds, randomFactor = 0.1) { () =>
        Source.futureSource {
          readOffset().map { offset =>
            log.infoN("Starting stream for tag [{}] from offset [{}]", tag, offset)
            processEventsByTag(offset, histogram)
              // groupedWithin can be used here to improve performance by reducing number of offset writes,
              // with the trade-off of possibility of more duplicate events when stream is restarted
              .mapAsync(1)(writeOffset)
          }
        }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)
  }

  private def processEventsByTag(offset: Offset, histogram: Histogram): Source[Offset, NotUsed] = {
    query.eventsByTag(tag, offset).mapAsync(1) { eventEnvelope =>
      eventEnvelope.event match {
        case event: Event =>
          {
            // Times from different nodes, take with a pinch of salt
            val latency = System.currentTimeMillis() - eventEnvelope.timestamp
            // when restarting without the offset the latency will be too big
            if (latency < histogram.getMaxValue) {
              histogram.recordValue(latency)
            }
            log.debugN(
              "Tag {} Event {} persistenceId {}, sequenceNr {}. Latency {}",
              tag,
              event,
              PersistenceId.ofUniqueId(eventEnvelope.persistenceId),
              eventEnvelope.sequenceNr,
              latency)
            Future.successful(Done)
          }.map(_ => eventEnvelope.offset)
        case other =>
          Future.failed(new IllegalArgumentException(s"Unexpected event [${other.getClass.getName}]"))
      }
    }
  }

  private def readOffset(): Future[Offset] = {
    session
      .selectOne(
        "SELECT timeUuidOffset FROM akka.offsetStore WHERE eventProcessorId = ? AND tag = ?",
        eventProcessorId,
        tag)
      .map(extractOffset)
  }

  private def extractOffset(maybeRow: Option[Row]): Offset = {
    maybeRow match {
      case Some(row) =>
        val uuid = row.getUuid("timeUuidOffset")
        if (uuid == null) {
          startOffset()
        } else {
          TimeBasedUUID(uuid)
        }
      case None => startOffset()
    }
  }

  // start looking from one week back if no offset was stored
  private def startOffset(): Offset = {
    query.timeBasedUUIDFrom(System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000))
  }

  private def prepareWriteOffset(): Future[PreparedStatement] = {
    session.prepare("INSERT INTO akka.offsetStore (eventProcessorId, tag, timeUuidOffset) VALUES (?, ?, ?)")
  }

  private def writeOffset(offset: Offset)(implicit ec: ExecutionContext): Future[Done] = {
    offset match {
      case t: TimeBasedUUID =>
        prepareWriteOffset().map(stmt => stmt.bind(eventProcessorId, tag, t.value)).flatMap { boundStmt =>
          session.executeWrite(boundStmt)
        }

      case _ =>
        throw new IllegalArgumentException(s"Unexpected offset type $offset")
    }

  }

}
