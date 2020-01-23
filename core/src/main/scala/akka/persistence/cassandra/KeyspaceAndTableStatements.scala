/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.collection.immutable

import akka.actor.ActorSystem

/**
 * Definitions of keyspace and table creation statements.
 */
class KeyspaceAndTableStatements(system: ActorSystem, configPath: String)
    extends CassandraStatements(new PluginSettings(system, system.settings.config.getConfig(configPath))) {

  /**
   * The Cassandra Statement that can be used to create the configured keyspace.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   */
  def createJournalKeyspaceStatement: String =
    journalStatements.createKeyspace

  /**
   * Scala API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   */
  def createJournalTablesStatements: immutable.Seq[String] =
    journalStatements.createTable ::
    journalStatements.createTagsTable ::
    journalStatements.createTagsProgressTable ::
    journalStatements.createTagScanningTable ::
    journalStatements.createMetadataTable ::
    Nil

  /**
   * Java API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   */
  def getCreateJournalTablesStatements: java.util.List[String] = {
    import scala.collection.JavaConverters._
    createJournalTablesStatements.asJava
  }

  /**
   * The Cassandra Statement that can be used to create the configured keyspace.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   */
  def createSnapshotKeyspaceStatement: String =
    snapshotStatements.createKeyspace

  /**
   * Scala API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   */
  def createSnapshotTablesStatements: immutable.Seq[String] =
    snapshotStatements.createTable :: Nil

  /**
   * Java API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   */
  def getCreateSnapshotTablesStatements: java.util.List[String] = {
    import scala.collection.JavaConverters._
    createSnapshotTablesStatements.asJava
  }

}
