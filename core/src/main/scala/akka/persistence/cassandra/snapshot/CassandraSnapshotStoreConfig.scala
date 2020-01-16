/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import scala.collection.immutable

import com.typesafe.config.Config
import akka.persistence.cassandra.CassandraPluginConfig
import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraPluginConfig.getReplicationStrategy
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.getListFromConfig

class CassandraSnapshotStoreConfig(system: ActorSystem, config: Config) extends CassandraPluginConfig(system, config) {
  private val snapshotConfig = config.getConfig("snapshot")

  val writeProfile: String = snapshotConfig.getString("write-profile")
  val readProfile: String = snapshotConfig.getString("read-profile")

  CassandraPluginConfig.checkProfile(system, readProfile)
  CassandraPluginConfig.checkProfile(system, writeProfile)

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

  /**
   * The Cassandra Statement[_]that can be used to create the configured keyspace.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-journal.snapshot")).createKeyspaceStatement
   * }}}
   *
   * @see [[CassandraSnapshotStoreConfig#createTablesStatements]]
   */
  def createKeyspaceStatement: String =
    statements.createKeyspace

  /**
   * Scala API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-journal.snapshot")).createTablesStatements
   * }}}
   * *
   * * @see [[CassandraSnapshotStoreConfig#createKeyspaceStatement]]
   */
  def createTablesStatements: immutable.Seq[String] =
    statements.createTable :: Nil

  /**
   * Java API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings().config().getConfig("cassandra-journal.snapshot")).getCreateTablesStatements();
   * }}}
   * *
   * * @see [[CassandraSnapshotStoreConfig#createKeyspaceStatement]]
   */
  def getCreateTablesStatements: java.util.List[String] = {
    import scala.collection.JavaConverters._
    createTablesStatements.asJava
  }

  private def statements: CassandraStatements =
    new CassandraStatements {
      override def snapshotConfig: CassandraSnapshotStoreConfig =
        CassandraSnapshotStoreConfig.this
    }
}
