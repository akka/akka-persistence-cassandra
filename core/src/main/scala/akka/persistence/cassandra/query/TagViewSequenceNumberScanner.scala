/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.util.UUID

import akka.NotUsed
import akka.annotation.InternalApi
import akka.event.Logging
import akka.pattern.after
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.query.TagViewSequenceNumberScanner.Session
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row }
import scala.annotation.nowarn
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

import akka.persistence.cassandra.BucketSize
import akka.stream.ActorAttributes
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TagViewSequenceNumberScanner {

  case class Session(session: CassandraSession, selectTagSequenceNumbers: PreparedStatement, profile: String) {
    private[akka] def selectTagSequenceNrs(
        tag: String,
        bucket: TimeBucket,
        from: UUID,
        to: UUID): Source[Row, NotUsed] = {
      val bound = selectTagSequenceNumbers.bind(tag, bucket.key: JLong, from, to).setExecutionProfileName(profile)
      session.select(bound)
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TagViewSequenceNumberScanner(session: Session, pluginDispatcher: String)(
    implicit materializer: Materializer,
    @nowarn("msg=never used") ec: ExecutionContext) {
  private val log = Logging(materializer.system, classOf[TagViewSequenceNumberScanner])

  /**
   * This could be its own stage and return half way through a query to better meet the deadline
   * but this is a quick and simple way to do it given we're scanning a small segment
   * @param fromOffset Exclusive
   * @param toOffset Inclusive
   * @param whichToKeep if multiple tag pid sequence nrs are found for the same tag/pid which to keep
   */
  private[akka] def scan(
      tag: String,
      fromOffset: UUID,
      toOffset: UUID,
      bucketSize: BucketSize,
      scanningPeriod: FiniteDuration,
      whichToKeep: (TagPidSequenceNr, TagPidSequenceNr) => TagPidSequenceNr)
      : Future[Map[PersistenceId, (TagPidSequenceNr, UUID)]] = {

    def doIt(): Future[Map[PersistenceId, (TagPidSequenceNr, UUID)]] = {

      // How many buckets is this spread across?
      val startBucket = TimeBucket(fromOffset, bucketSize)
      val endBucket = TimeBucket(toOffset, bucketSize)

      require(startBucket <= endBucket)

      if (log.isDebugEnabled) {
        log.debug(
          s"Scanning tag: $tag from: {}, to: {}. Bucket {} to {}",
          formatOffset(fromOffset),
          formatOffset(toOffset),
          startBucket,
          endBucket)
      }

      Source
        .unfold(startBucket)(current => {
          if (current <= endBucket) {
            Some((current.next(), current))
          } else {
            None
          }
        })
        .flatMapConcat(bucket => {
          log.debug("Scanning bucket {}", bucket)
          session.selectTagSequenceNrs(tag, bucket, fromOffset, toOffset)
        })
        .map(row => (row.getString("persistence_id"), row.getLong("tag_pid_sequence_nr"), row.getUuid("timestamp")))
        .toMat(Sink.fold(Map.empty[Tag, (TagPidSequenceNr, UUID)]) {
          case (acc, (pid, tagPidSequenceNr, timestamp)) =>
            val (newTagPidSequenceNr, newTimestamp) = acc.get(pid) match {
              case None =>
                (tagPidSequenceNr, timestamp)
              case Some((currentTagPidSequenceNr, currentTimestamp)) =>
                if (whichToKeep(tagPidSequenceNr, currentTagPidSequenceNr) == tagPidSequenceNr)
                  (tagPidSequenceNr, timestamp)
                else
                  (currentTagPidSequenceNr, currentTimestamp)
            }
            acc + (pid -> ((newTagPidSequenceNr, newTimestamp)))
        })(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(pluginDispatcher))
        .run()
    }

    if (scanningPeriod > Duration.Zero) {
      after(scanningPeriod)(doIt())(materializer.system)
    } else {
      doIt()
    }

  }
}
