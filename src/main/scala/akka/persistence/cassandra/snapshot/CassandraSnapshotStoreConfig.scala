/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config
import akka.persistence.cassandra.CassandraPluginConfig
import akka.actor.ActorSystem

class CassandraSnapshotStoreConfig(system: ActorSystem, config: Config) extends CassandraPluginConfig(system, config) {
  val maxLoadAttempts = config.getInt("max-load-attempts")
  val cassandra2xCompat: Boolean = config.getBoolean("cassandra-2x-compat")
}
