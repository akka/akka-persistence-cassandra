package akka.persistence.cassandra.journal

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val maxPartitionSize: Int = config.getInt("max-partition-size") // TODO: make persistent
  val maxResultSize: Int = config.getInt("max-result-size")
}
