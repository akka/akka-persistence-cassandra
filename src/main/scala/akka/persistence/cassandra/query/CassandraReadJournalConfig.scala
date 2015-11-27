package akka.persistence.cassandra.query

import scala.concurrent.duration._
import com.typesafe.config.Config
import com.datastax.driver.core.ConsistencyLevel
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.TimeBucket

private[query] class CassandraReadJournalConfig(config: Config, writePluginConfig: CassandraJournalConfig) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval", MILLISECONDS).millis
  val maxBufferSize: Int = config.getInt("max-buffer-size")
  val fetchSize: Int = config.getInt("fetch-size")
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
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
}
