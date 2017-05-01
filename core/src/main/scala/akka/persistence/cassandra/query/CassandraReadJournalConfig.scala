/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.actor.NoSerializationVerificationNeeded
import scala.concurrent.duration._
import com.typesafe.config.Config
import com.datastax.driver.core.ConsistencyLevel
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.TimeBucket

private[query] class CassandraReadJournalConfig(config: Config, writePluginConfig: CassandraJournalConfig)
  extends NoSerializationVerificationNeeded {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval", MILLISECONDS).millis
  val maxBufferSize: Int = config.getInt("max-buffer-size")
  val fetchSize: Int = config.getInt("max-result-size-query")
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val readRetries: Int = config.getInt("read-retries")
  val firstTimeBucket: TimeBucket = TimeBucket(config.getString("first-time-bucket"))
  val eventualConsistencyDelay: FiniteDuration =
    config.getDuration("eventual-consistency-delay", MILLISECONDS).millis
  val delayedEventTimeout: FiniteDuration =
    config.getDuration("delayed-event-timeout", MILLISECONDS).millis
  val pluginDispatcher: String = config.getString("plugin-dispatcher")

  val eventsByTagView: String = writePluginConfig.eventsByTagView
  val keyspace: String = writePluginConfig.keyspace
  val targetPartitionSize: Int = writePluginConfig.targetPartitionSize
  val table: String = writePluginConfig.table
  val pubsubMinimumInterval: Duration = writePluginConfig.pubsubMinimumInterval

}
