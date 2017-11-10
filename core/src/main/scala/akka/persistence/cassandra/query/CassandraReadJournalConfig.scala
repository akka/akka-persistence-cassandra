/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.format.DateTimeFormatter
import java.time.{ LocalDate, ZoneOffset }

import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, TimeBucket }
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraReadJournalConfig(config: Config, writePluginConfig: CassandraJournalConfig)
  extends NoSerializationVerificationNeeded {

  private val timeBucketFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)

  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval", MILLISECONDS).millis
  val gapFreeSequenceNumbers: Boolean = config.getBoolean("gap-free-sequence-numbers")
  val maxBufferSize: Int = config.getInt("max-buffer-size")
  val fetchSize: Int = config.getInt("max-result-size-query")

  // TODO use for the events by tag query too
  val fetchMoreThreshold: Double = config.getDouble("fetch-more-threshold")
  require(
    0.0 <= fetchMoreThreshold && fetchMoreThreshold <= 1.0,
    s"fetch-more-threshold must be between 0.0 and 1.0, was $fetchMoreThreshold"
  )
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val readRetries: Int = config.getInt("read-retries")

  val firstTimeBucket: TimeBucket = {
    val date: LocalDate = LocalDate.parse(config.getString("first-time-bucket"), timeBucketFormatter)
    TimeBucket(
      date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli,
      writePluginConfig.bucketSize
    )
  }

  val pluginDispatcher: String = config.getString("plugin-dispatcher")

  val keyspace: String = writePluginConfig.keyspace
  val targetPartitionSize: Int = writePluginConfig.targetPartitionSize
  val table: String = writePluginConfig.table
  val pubsubNotification: Boolean = writePluginConfig.tagWriterSettings.pubsubNotification
  val eventsByPersistenceIdEventTimeout: FiniteDuration = config.getDuration("events-by-persistence-id-gap-timeout", MILLISECONDS).millis
}
