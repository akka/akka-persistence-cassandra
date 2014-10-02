package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraSnapshotStoreConfig(config: Config) extends CassandraPluginConfig(config) {
  val maxMetadataResultSize = config.getInt("max-metadata-result-size")
}
