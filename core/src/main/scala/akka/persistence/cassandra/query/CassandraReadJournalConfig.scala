/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }

import akka.actor.ActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Day, Hour, TimeBucket }
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraReadJournalConfig(
    system: ActorSystem,
    config: Config,
    writePluginConfig: CassandraJournalConfig)
    extends NoSerializationVerificationNeeded {

  val log = Logging(system, classOf[CassandraReadJournalConfig])

  val refreshInterval: FiniteDuration =
    config.getDuration("refresh-interval", MILLISECONDS).millis
  val gapFreeSequenceNumbers: Boolean =
    config.getBoolean("gap-free-sequence-numbers")
  val maxBufferSize: Int = config.getInt("max-buffer-size")
  val fetchSize: Int = config.getInt("max-result-size-query")

  // TODO use for the events by tag query too
  val fetchMoreThreshold: Double = config.getDouble("fetch-more-threshold")
  require(
    0.0 <= fetchMoreThreshold && fetchMoreThreshold <= 1.0,
    s"fetch-more-threshold must be between 0.0 and 1.0, was $fetchMoreThreshold")
  val readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val readRetries: Int = config.getInt("read-retries")

  val firstTimeBucket: TimeBucket = {
    val firstBucket = config.getString("first-time-bucket")
    val firstBucketPadded = (writePluginConfig.bucketSize, firstBucket) match {
      case (_, fb) if fb.length == 14    => fb
      case (Hour, fb) if fb.length == 11 => s"${fb}:00"
      case (Day, fb) if fb.length == 8   => s"${fb}T00:00"
      case _ =>
        throw new IllegalArgumentException("Invalid first-time-bucket format. Use: " + firstBucketFormat)
    }
    val date: LocalDateTime =
      LocalDateTime.parse(firstBucketPadded, firstBucketFormatter)
    TimeBucket(date.toInstant(ZoneOffset.UTC).toEpochMilli, writePluginConfig.bucketSize)
  }

  val deserializationParallelism: Int =
    config.getInt("deserialization-parallelism")

  val pluginDispatcher: String = config.getString("plugin-dispatcher")

  val keyspace: String = writePluginConfig.keyspace
  val targetPartitionSize: Long = writePluginConfig.targetPartitionSize
  val table: String = writePluginConfig.table
  val pubsubNotification: Duration =
    writePluginConfig.tagWriterSettings.pubsubNotification
  val eventsByPersistenceIdEventTimeout: FiniteDuration =
    config.getDuration("events-by-persistence-id-gap-timeout", MILLISECONDS).millis

  val eventsByTagGapTimeout: FiniteDuration =
    config.getDuration("events-by-tag.gap-timeout", MILLISECONDS).millis
  val eventsByTagDebug: Boolean =
    config.getBoolean("events-by-tag.verbose-debug-logging")
  val eventsByTagEventualConsistency: FiniteDuration =
    config.getDuration("events-by-tag.eventual-consistency-delay", MILLISECONDS).millis
  val eventsByTagNewPersistenceIdScanTimeout: FiniteDuration =
    config.getDuration("events-by-tag.new-persistence-id-scan-timeout", MILLISECONDS).millis
  val eventsByTagOffsetScanning: FiniteDuration =
    config.getDuration("events-by-tag.offset-scanning-period", MILLISECONDS).millis
  val eventsByTagCleanUpPersistenceIds: Duration =
    config.getString("events-by-tag.cleanup-old-persistence-ids").toLowerCase match {
      case "off" | "false" => Duration.Inf
      case "<default>"     => (writePluginConfig.bucketSize.durationMillis * 2).millis
      case _               => config.getDuration("events-by-tag.cleanup-old-persistence-ids", MILLISECONDS).millis
    }

  if (eventsByTagCleanUpPersistenceIds != Duration.Inf && eventsByTagCleanUpPersistenceIds.toMillis < (writePluginConfig.bucketSize.durationMillis * 2)) {
    log.warning(
      "cleanup-old-persistence-ids has been set to less than 2 x the bucket size. If a tagged event for a persistence id " +
      "is not received for the cleanup period but then received before 2 x the bucket size then old events could re-delivered.")
  }
}
