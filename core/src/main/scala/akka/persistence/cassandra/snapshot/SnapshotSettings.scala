/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.cassandra.PluginSettings.getReplicationStrategy
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.getListFromConfig
import com.typesafe.config.Config

/** INTERNAL API */
@InternalApi private[akka] class SnapshotSettings(system: ActorSystem, config: Config) {
  private val snapshotConfig = config.getConfig("snapshot")

  val writeProfile: String = snapshotConfig.getString("write-profile")
  val readProfile: String = snapshotConfig.getString("read-profile")

  val keyspaceAutoCreate: Boolean = snapshotConfig.getBoolean("keyspace-autocreate")
  val tablesAutoCreate: Boolean = snapshotConfig.getBoolean("tables-autocreate")

  val keyspace: String = snapshotConfig.getString("keyspace")

  val table: String = snapshotConfig.getString("table")

  val tableCompactionStrategy: CassandraCompactionStrategy =
    CassandraCompactionStrategy(snapshotConfig.getConfig("table-compaction-strategy"))

  val replicationStrategy: String = getReplicationStrategy(
    snapshotConfig.getString("replication-strategy"),
    snapshotConfig.getInt("replication-factor"),
    getListFromConfig(snapshotConfig, "data-center-replication-factors"))

  val gcGraceSeconds: Long = snapshotConfig.getLong("gc-grace-seconds")

  val maxLoadAttempts: Int = snapshotConfig.getInt("max-load-attempts")

}
