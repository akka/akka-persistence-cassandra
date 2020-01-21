/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.firstBucketFormatter
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventsByTagSettings {
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
  final case class BackTrackSettings(
      metadataCleanupInterval: Option[FiniteDuration],
      interval: Option[FiniteDuration],
      period: Period,
      longInterval: Option[FiniteDuration],
      longPeriod: Period) {

    validatePeriodLengths()
    validateIntervals()

    private def validateIntervals(): Unit = {
      (interval, longInterval) match {
        case (None, Some(_)) =>
          throw new IllegalArgumentException(
            "interval must be enabled to use long-interval. If you want a single interval just set interval, not long-interval")
        case (Some(i), Some(li)) => require(i < li, "interval must be smaller than long-interval")
        case _                   =>
      }
    }
    private def validatePeriodLengths(): Unit = {
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
@InternalApi private[akka] class EventsByTagSettings(system: ActorSystem, config: Config) {
  import EventsByTagSettings._
  private val eventsByTagConfig = config.getConfig("events-by-tag")
  private val log = Logging(system, classOf[EventsByTagSettings])

  val eventsByTagEnabled: Boolean = eventsByTagConfig.getBoolean("enabled")

  val bucketSize: BucketSize =
    BucketSize.fromString(eventsByTagConfig.getString("bucket-size"))

  if (bucketSize == Second) {
    system.log.warning("Do not use Second bucket size in production. It is meant for testing purposes only.")
  }

  val tagTable = TableSettings(
    eventsByTagConfig.getString("table"),
    CassandraCompactionStrategy(eventsByTagConfig.getConfig("compaction-strategy")),
    eventsByTagConfig.getLong("gc-grace-seconds"),
    if (eventsByTagConfig.hasPath("time-to-live"))
      Some(eventsByTagConfig.getDuration("time-to-live", TimeUnit.MILLISECONDS).millis)
    else None)

  val pubsubNotification: Duration =
    eventsByTagConfig.getString("pubsub-notification").toLowerCase match {
      case "on" | "true"   => 100.millis
      case "off" | "false" => Duration.Undefined
      case _               => eventsByTagConfig.getDuration("pubsub-notification", TimeUnit.MILLISECONDS).millis
    }

  val tagWriterSettings = TagWriterSettings(
    eventsByTagConfig.getInt("max-message-batch-size"),
    eventsByTagConfig.getDuration("flush-interval", TimeUnit.MILLISECONDS).millis,
    eventsByTagConfig.getDuration("scanning-flush-interval", TimeUnit.MILLISECONDS).millis,
    eventsByTagConfig.getDuration("stop-tag-writer-when-idle", TimeUnit.MILLISECONDS).millis,
    pubsubNotification)

  val firstTimeBucket: TimeBucket = {
    val firstBucket = eventsByTagConfig.getString("first-time-bucket")
    val firstBucketPadded = (bucketSize, firstBucket) match {
      case (_, fb) if fb.length == 14    => fb
      case (Hour, fb) if fb.length == 11 => s"$fb:00"
      case (Day, fb) if fb.length == 8   => s"${fb}T00:00"
      case _ =>
        throw new IllegalArgumentException("Invalid first-time-bucket format. Use: " + query.firstBucketFormat)
    }
    val date: LocalDateTime =
      LocalDateTime.parse(firstBucketPadded, firstBucketFormatter)
    TimeBucket(date.toInstant(ZoneOffset.UTC).toEpochMilli, bucketSize)
  }

  val eventsByTagGapTimeout: FiniteDuration =
    eventsByTagConfig.getDuration("gap-timeout", MILLISECONDS).millis

  val verboseDebug: Boolean =
    eventsByTagConfig.getBoolean("verbose-debug-logging")

  val eventualConsistency: FiniteDuration =
    eventsByTagConfig.getDuration("eventual-consistency-delay", MILLISECONDS).millis

  val newPersistenceIdScanTimeout: FiniteDuration =
    eventsByTagConfig.getDuration("new-persistence-id-scan-timeout", MILLISECONDS).millis

  val offsetScanning: FiniteDuration =
    eventsByTagConfig.getDuration("offset-scanning-period", MILLISECONDS).millis

  val cleanUpPersistenceIds: Option[FiniteDuration] =
    eventsByTagConfig.getString("cleanup-old-persistence-ids").toLowerCase match {
      case "off" | "false" => None
      case "<default>"     => Some((bucketSize.durationMillis * 2).millis)
      case _               => Some(eventsByTagConfig.getDuration("cleanup-old-persistence-ids", MILLISECONDS).millis)
    }

  if (cleanUpPersistenceIds.exists(_.toMillis < (bucketSize.durationMillis * 2))) {
    log.warning(
      "cleanup-old-persistence-ids has been set to less than 2 x the bucket size. If a tagged event for a persistence id " +
      "is not received for the cleanup period but then received before 2 x the bucket size then old events could re-delivered.")
  }

  val backtrack = BackTrackSettings(
    cleanUpPersistenceIds,
    optionalDuration(eventsByTagConfig, "back-track.interval"),
    period(eventsByTagConfig, "back-track.period"),
    optionalDuration(eventsByTagConfig, "back-track.long-interval"),
    period(eventsByTagConfig, "back-track.long-period"))

  private def optionalDuration(cfg: Config, path: String): Option[FiniteDuration] = {
    cfg.getString(path).toLowerCase match {
      case "off" | "false" => None
      case _               => Some(cfg.getDuration(path, MILLISECONDS).millis)
    }
  }

  private def period(cfg: Config, path: String): Period = {
    cfg.getString(path).toLowerCase match {
      case "max" => Max
      case _     => Fixed(cfg.getDuration(path, MILLISECONDS).millis)
    }
  }

}

/** INTERNAL API */
@InternalApi private[akka] case class TableSettings(
    name: String,
    compactionStrategy: CassandraCompactionStrategy,
    gcGraceSeconds: Long,
    ttl: Option[Duration])

/** INTERNAL API */
@InternalApi private[akka] sealed trait BucketSize {
  val durationMillis: Long
}

/** INTERNAL API */
@InternalApi private[akka] case object Day extends BucketSize {
  override val durationMillis: Long = 1.day.toMillis
}

/** INTERNAL API */
@InternalApi private[akka] case object Hour extends BucketSize {
  override val durationMillis: Long = 1.hour.toMillis
}

/** INTERNAL API */
@InternalApi private[akka] case object Minute extends BucketSize {
  override val durationMillis: Long = 1.minute.toMillis
}

/**
 * INTERNAL API
 * Not to be used for real production apps. Just to make testing bucket transitions easier.
 */
@InternalApi private[akka] case object Second extends BucketSize {
  override val durationMillis: Long = 1.second.toMillis
}

/** INTERNAL API */
@InternalApi private[akka] object BucketSize {
  def fromString(value: String): BucketSize =
    Vector(Day, Hour, Minute, Second)
      .find(_.toString.toLowerCase == value.toLowerCase)
      .getOrElse(throw new IllegalArgumentException("Invalid value for bucket size: " + value))
}
