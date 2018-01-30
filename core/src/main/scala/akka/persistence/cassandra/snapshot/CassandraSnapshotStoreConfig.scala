/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config
import akka.persistence.cassandra.CassandraPluginConfig
import akka.actor.ActorSystem

class CassandraSnapshotStoreConfig(system: ActorSystem, config: Config) extends CassandraPluginConfig(system, config) {
  val maxLoadAttempts = config.getInt("max-load-attempts")
}
