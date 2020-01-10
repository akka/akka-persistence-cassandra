/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }

import akka.actor.{ ActorSystem, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.cassandra.session.CqlSessionProvider
import akka.event.Logging
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Day, Hour, TimeBucket }
import akka.persistence.cassandra.query.CassandraReadJournalConfig.{ BackTrackConfig, Fixed, Max }
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi object CassandraReadJournalConfig {

  sealed trait Period
  case object Max extends Period
  case class Fixed(duration: Duration) extends Period

  case class BackTrackConfig(interval: Duration, period: Period)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraReadJournalConfig(
    system: ActorSystem,
    config: Config,
    writePluginConfig: CassandraJournalConfig)
    extends NoSerializationVerificationNeeded {

  val log = Logging(system, classOf[CassandraReadJournalConfig])

  val readProfile = config.getString("read-profile")
  CassandraPluginConfig.checkProfile(system, readProfile)

  val sessionProvider: CqlSessionProvider = CqlSessionProvider(system.asInstanceOf[ExtendedActorSystem], config)

  val refreshInterval: FiniteDuration =
    config.getDuration("refresh-interval", MILLISECONDS).millis

  val gapFreeSequenceNumbers: Boolean = config.getBoolean("gap-free-sequence-numbers")

  val maxBufferSize: Int = config.getInt("max-buffer-size")

  val firstTimeBucket: TimeBucket = {
    val firstBucket = config.getString("first-time-bucket")
    val firstBucketPadded = (writePluginConfig.bucketSize, firstBucket) match {
      case (_, fb) if fb.length == 14    => fb
      case (Hour, fb) if fb.length == 11 => s"$fb:00"
      case (Day, fb) if fb.length == 8   => s"${fb}T00:00"
      case _ =>
        throw new IllegalArgumentException("Invalid first-time-bucket format. Use: " + firstBucketFormat)
    }
    val date: LocalDateTime =
      LocalDateTime.parse(firstBucketPadded, firstBucketFormatter)
    TimeBucket(date.toInstant(ZoneOffset.UTC).toEpochMilli, writePluginConfig.bucketSize)
  }

  val deserializationParallelism: Int = config.getInt("deserialization-parallelism")

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

  val eventsByTagBacktrack = BackTrackConfig(config.getString("events-by-tag.back-track.interval").toLowerCase match {
    case "off" | "false" => Duration.Inf
    case _               => config.getDuration("events-by-tag.back-track.interval", MILLISECONDS).millis
  }, config.getString("events-by-tag.back-track.period") match {
    case "max" => Max
    case _     => Fixed(config.getDuration("events-by-tag.back-track.period", MILLISECONDS).millis)
  })

  if (eventsByTagCleanUpPersistenceIds != Duration.Inf && eventsByTagCleanUpPersistenceIds.toMillis < (writePluginConfig.bucketSize.durationMillis * 2)) {
    log.warning(
      "cleanup-old-persistence-ids has been set to less than 2 x the bucket size. If a tagged event for a persistence id " +
      "is not received for the cleanup period but then received before 2 x the bucket size then old events could re-delivered.")
  }
}
