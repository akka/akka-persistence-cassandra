/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.ActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.cassandra.PluginSettings.getReplicationStrategy
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.getListFromConfig
import com.typesafe.config.Config

/** INTERNAL API */
@InternalStableApi
@InternalApi private[akka] class JournalSettings(system: ActorSystem, config: Config)
    extends NoSerializationVerificationNeeded {

  private val journalConfig = config.getConfig("journal")

  val writeProfile: String = config.getString("write-profile")
  val readProfile: String = config.getString("read-profile")

  val keyspaceAutoCreate: Boolean = journalConfig.getBoolean("keyspace-autocreate")
  val tablesAutoCreate: Boolean = journalConfig.getBoolean("tables-autocreate")

  val keyspace: String = journalConfig.getString("keyspace")

  val table: String = journalConfig.getString("table")
  val metadataTable: String = journalConfig.getString("metadata-table")
  val allPersistenceIdsTable: String = journalConfig.getString("all-persistence-ids-table")

  val tableCompactionStrategy: CassandraCompactionStrategy =
    CassandraCompactionStrategy(journalConfig.getConfig("table-compaction-strategy"))

  val replicationStrategy: String = getReplicationStrategy(
    journalConfig.getString("replication-strategy"),
    journalConfig.getInt("replication-factor"),
    getListFromConfig(journalConfig, "data-center-replication-factors"))

  val gcGraceSeconds: Long = journalConfig.getLong("gc-grace-seconds")

  val targetPartitionSize: Long = journalConfig.getLong("target-partition-size")
  val maxMessageBatchSize: Int = journalConfig.getInt("max-message-batch-size")

  val maxConcurrentDeletes: Int = journalConfig.getInt("max-concurrent-deletes")

  val supportDeletes: Boolean = journalConfig.getBoolean("support-deletes")

  val supportAllPersistenceIds: Boolean = journalConfig.getBoolean("support-all-persistence-ids")

  val coordinatedShutdownOnError: Boolean = config.getBoolean("coordinated-shutdown-on-error")

}
