package akka.persistence.cassandra.journal

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val maxPartitionSize: Int = config.getInt(CassandraJournalConfig.MaxPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
}

object CassandraJournalConfig {
  val MaxPartitionProperty: String = "max-partition-size"
}
