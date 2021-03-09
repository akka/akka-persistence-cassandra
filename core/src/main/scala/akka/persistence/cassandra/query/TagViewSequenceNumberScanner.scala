/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.lang.{ Long => JLong }
import java.util.UUID

import akka.NotUsed
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal.BucketSize
import akka.persistence.cassandra.query.TagViewSequenceNumberScanner.Session
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Row
import scala.concurrent.duration.{ Deadline, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

import akka.stream.ActorAttributes
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink

@InternalApi
private[akka] object TagViewSequenceNumberScanner {

  case class Session(session: CassandraSession, selectTagSequenceNumbers: PreparedStatement) {
    private[akka] def selectTagSequenceNrs(
        tag: String,
        bucket: TimeBucket,
        from: UUID,
        to: UUID): Source[Row, NotUsed] = {
      val bound = selectTagSequenceNumbers.bind(tag, bucket.key: JLong, from, to)
      session.select(bound)
    }
  }

}

@InternalApi
private[akka] class TagViewSequenceNumberScanner(session: Session, pluginDispatcher: String)(
    implicit materializer: ActorMaterializer,
    ec: ExecutionContext) {
  private val log = Logging(materializer.system, getClass)

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
    val deadline: Deadline = Deadline.now + scanningPeriod

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
        .map(row => (row.getString("persistence_id"), row.getLong("tag_pid_sequence_nr"), row.getUUID("timestamp")))
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
        .flatMap { result =>
          if (deadline.hasTimeLeft()) {
            doIt()
          } else {
            Future.successful(result)
          }
        }
    }
    doIt()
  }
}
