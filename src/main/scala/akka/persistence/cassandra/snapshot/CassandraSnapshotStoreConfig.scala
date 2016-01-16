/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraSnapshotStoreConfig(config: Config) extends CassandraPluginConfig(config) {
  val maxMetadataResultSize = config.getInt("max-metadata-result-size")
}
