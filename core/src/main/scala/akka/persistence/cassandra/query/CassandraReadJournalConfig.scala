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
import akka.persistence.cassandra.query.CassandraReadJournalConfig.Period
import akka.persistence.cassandra.query.CassandraReadJournalConfig.{ BackTrackConfig, Fixed, Max }
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi object CassandraReadJournalConfig {

  sealed trait Period
  case object Max extends Period
  case class Fixed(duration: FiniteDuration) extends Period

  /**
   * Stores and validates the back track configuration.
   *
   * Back tacking can not go further back than the persistence id clean up timeout as it would deliver
   * previously delivered events for which the metadata has been dropped.
   * This is validated at start up.
   *
   * Back tracking can not go further back than the start of the previous bucket as missing searches do not
   * go back any further than that. The duration to the start of the previous bucket is constantly changing
   * with time so it is passed into the period methods by the events by tag stage.
   *
   * @param metadataCleanupInterval How often metadata about persistence ids is cleanled up
   * @param interval How often to back track
   * @param period How far to back track
   * @param longInterval A less frequent back track and can be used to go back further
   * @param longPeriod How far to back track for the long interval
   */
  final case class BackTrackConfig(
      metadataCleanupInterval: Option[FiniteDuration],
      interval: Option[FiniteDuration],
      period: Period,
      longInterval: Option[FiniteDuration],
      longPeriod: Period) {

    validatePeriodLengths()
    validateIntervals()

    private def validateIntervals() = {
      (interval, longInterval) match {
        case (None, Some(_)) =>
          throw new IllegalArgumentException(
            "interval must be enabled to use long-interval. If you want a single interval just set interval, not long-interval")
        case (Some(i), Some(li)) => require(i < li, "interval must be smaller than long-interval")
        case _                   =>
      }
    }
    private def validatePeriodLengths() = {
      def cleanUpGreaterThanPeriod(period: Period, name: String): Unit = {
        (metadataCleanupInterval, period) match {
          case (Some(cleanup), Fixed(p)) =>
            require((cleanup * 0.9) > p, s"$name has to be at least 10% lower than cleanup-old-persistence-ids")
          case _ =>
        }
      }
      cleanUpGreaterThanPeriod(period, "period")
      cleanUpGreaterThanPeriod(longPeriod, "long-period")
    }

    private def intervalToMillis(interval: Option[FiniteDuration]): Long = {
      interval match {
        case None           => Long.MaxValue
        case Some(interval) => interval.toMillis
      }
    }

    private def calculatePeriod(from: Long, period: Period, startOfPreviousBucket: Long): Long = {
      period match {
        case Max =>
          metadataCleanupInterval match {
            case None                  => startOfPreviousBucket
            case Some(cleanupInterval) => math.max(from - cleanupInterval.toMillis, startOfPreviousBucket)
          }
        case Fixed(p) => math.max(from - p.toMillis, startOfPreviousBucket)
      }
    }

    def intervalMillis(): Long = intervalToMillis(interval)
    def longIntervalMillis(): Long = intervalToMillis(longInterval)

    def periodMillis(from: Long, startOfPreviousBucket: Long): Long =
      calculatePeriod(from, period, startOfPreviousBucket)
    def longPeriodMillis(from: Long, startOfPreviousBucket: Long): Long =
      calculatePeriod(from, longPeriod, startOfPreviousBucket)
  }
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
  val eventsByTagCleanUpPersistenceIds: Option[FiniteDuration] =
    config.getString("events-by-tag.cleanup-old-persistence-ids").toLowerCase match {
      case "off" | "false" => None
      case "<default>"     => Some((writePluginConfig.bucketSize.durationMillis * 2).millis)
      case _               => Some(config.getDuration("events-by-tag.cleanup-old-persistence-ids", MILLISECONDS).millis)
    }

  if (eventsByTagCleanUpPersistenceIds.exists(_.toMillis < (writePluginConfig.bucketSize.durationMillis * 2))) {
    log.warning(
      "cleanup-old-persistence-ids has been set to less than 2 x the bucket size. If a tagged event for a persistence id " +
      "is not received for the cleanup period but then received before 2 x the bucket size then old events could re-delivered.")
  }

  val eventsByTagBacktrack = BackTrackConfig(
    eventsByTagCleanUpPersistenceIds,
    optionalDuration("events-by-tag.back-track.interval"),
    period("events-by-tag.back-track.period"),
    optionalDuration("events-by-tag.back-track.long-interval"),
    period("events-by-tag.back-track.long-period"))

  private def optionalDuration(path: String): Option[FiniteDuration] = {
    config.getString(path).toLowerCase match {
      case "off" | "false" => None
      case _               => Some(config.getDuration(path, MILLISECONDS).millis)
    }
  }

  private def period(path: String): Period = {
    config.getString(path).toLowerCase match {
      case "max" => Max
      case _     => Fixed(config.getDuration(path, MILLISECONDS).millis)
    }
  }

}
