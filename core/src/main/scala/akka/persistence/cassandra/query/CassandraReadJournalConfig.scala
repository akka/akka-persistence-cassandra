/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }

import akka.actor.{ ActorSystem, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.cassandra.session.CqlSessionProvider
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Day, Hour, TimeBucket }
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

  val readProfile = config.getString("read-profile")
  CassandraPluginConfig.checkProfile(system, readProfile)

  val sessionProvider: CqlSessionProvider = CqlSessionProvider(system.asInstanceOf[ExtendedActorSystem], config)

  val refreshInterval: FiniteDuration =
    config.getDuration("refresh-interval", MILLISECONDS).millis

  val gapFreeSequenceNumbers: Boolean =
    config.getBoolean("gap-free-sequence-numbers")
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

}
