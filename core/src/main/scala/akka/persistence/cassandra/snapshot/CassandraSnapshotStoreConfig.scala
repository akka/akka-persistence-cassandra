/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import scala.collection.immutable

import com.typesafe.config.Config
import akka.persistence.cassandra.CassandraPluginConfig
import akka.actor.ActorSystem

class CassandraSnapshotStoreConfig(system: ActorSystem, config: Config) extends CassandraPluginConfig(system, config) {
  val maxLoadAttempts = config.getInt("max-load-attempts")
  val cassandra2xCompat = config.getBoolean("cassandra-2x-compat")

  /**
   * The Cassandra Statement[_]that can be used to create the configured keyspace.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-snapshot-store")).createKeyspaceStatement
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
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-snapshot-store")).createTablesStatements
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
   * new CassandraSnapshotStoreConfig(actorSystem, actorSystem.settings().config().getConfig("cassandra-snapshot-store")).getCreateTablesStatements();
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
