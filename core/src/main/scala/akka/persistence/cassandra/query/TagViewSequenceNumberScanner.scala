/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.util.UUID

import akka.annotation.InternalApi
import akka.cassandra.session.scaladsl.CassandraSession
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.formatOffset
import akka.stream.ActorMaterializer
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.uuid.Uuids

import scala.concurrent.duration.{ Deadline, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

@InternalApi
private[akka] class TagViewSequenceNumberScanner(
    session: CassandraSession,
    profile: String,
    ps: Future[PreparedStatement])(implicit materializer: ActorMaterializer, ec: ExecutionContext) {
  private val log = Logging(materializer.system, getClass)

  /**
   * This could be its own stage and return half way through a query to better meet the deadline
   * but this is a quick and simple way to do it given we're scanning a small segment
   */
  private[akka] def scan(
      tag: String,
      offset: UUID,
      bucket: TimeBucket,
      scanningPeriod: FiniteDuration): Future[Map[PersistenceId, (TagPidSequenceNr, UUID)]] =
    ps.flatMap(ps => {
      val deadline: Deadline = Deadline.now + scanningPeriod
      val to = Uuids.endOf(System.currentTimeMillis() + scanningPeriod.toMillis)

      def doIt(): Future[Map[Tag, (TagPidSequenceNr, UUID)]] = {
        val bound = ps.bind(tag, bucket.key: JLong, offset, to).setExecutionProfileName(profile)
        log.debug(
          "Scanning tag: {} bucket: {}, from: {}, to: {}",
          tag,
          bucket.key,
          formatOffset(offset),
          formatOffset(to))
        val source = session.select(bound)
        val doneIt = source
          .map(row => (row.getString("persistence_id"), row.getLong("tag_pid_sequence_nr"), row.getUuid("timestamp")))
          .runFold(Map.empty[Tag, (TagPidSequenceNr, UUID)]) {
            case (acc, (pid, tagPidSequenceNr, timestamp)) =>
              val (newTagPidSequenceNr, newTimestamp) = acc.get(pid) match {
                case None =>
                  (tagPidSequenceNr, timestamp)
                case Some((currentTagPidSequenceNr, currentTimestamp)) =>
                  if (tagPidSequenceNr < currentTagPidSequenceNr)
                    (tagPidSequenceNr, timestamp)
                  else
                    (currentTagPidSequenceNr, currentTimestamp)
              }
              acc + (pid -> ((newTagPidSequenceNr, newTimestamp)))
          }
        doneIt.flatMap { result =>
          if (deadline.hasTimeLeft()) {
            doIt()
          } else {
            Future.successful(result)
          }
        }
      }
      // Subtract 1 so events by tag looks for the lowest tagPidSequenceNumber that was found during initial scanning
      // Make a fake UUID for this tagPidSequenceNr that will be used to search for this tagPidSequenceNr in the unlikely
      // event that the stage can't find the event found during this scan
      doIt().map { progress =>
        progress.map {
          case (key, (tagPidSequenceNr, uuid)) =>
            val unixTime = Uuids.unixTimestamp(uuid)
            (key, (tagPidSequenceNr - 1, Uuids.startOf(unixTime - 1)))
        }
      }
    })
}
