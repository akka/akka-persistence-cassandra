/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.cassandra.healthcheck.HealthCheckSettings
import akka.persistence.cassandra.journal.JournalSettings
import akka.persistence.cassandra.query.QuerySettings
import akka.persistence.cassandra.snapshot.SnapshotSettings
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi private[akka] class PluginSettings(system: ActorSystem, config: Config) {

  val journalSettings: JournalSettings = new JournalSettings(system, config)

  val eventsByTagSettings: EventsByTagSettings = new EventsByTagSettings(system, config)

  val querySettings: QuerySettings = new QuerySettings(system, config, eventsByTagSettings)

  val snapshotSettings: SnapshotSettings = new SnapshotSettings(system, config)

  val healthCheckSettings: HealthCheckSettings = new HealthCheckSettings(system, config)

  val cosmosDb: Boolean = config.getBoolean("compatibility.cosmosdb")
}

/**
 * INTERNAL API
 */
@InternalStableApi
@InternalApi private[akka] object PluginSettings {

  val DefaultConfigPath = "akka.persistence.cassandra"

  def apply(system: ActorSystem): PluginSettings =
    apply(system, system.settings.config.getConfig(DefaultConfigPath))

  def apply(system: ActorSystem, config: Config): PluginSettings =
    new PluginSettings(system, config)

  val keyspaceNameRegex =
    """^("[a-zA-Z]{1}[\w]{0,47}"|[a-zA-Z]{1}[\w]{0,47})$"""

  /**
   * Builds replication strategy command to create a keyspace.
   */
  def getReplicationStrategy(
      strategy: String,
      replicationFactor: Int,
      dataCenterReplicationFactors: Seq[String]): String = {

    def getDataCenterReplicationFactorList(dcrfList: Seq[String]): String = {
      val result: Seq[String] = dcrfList match {
        case null | Nil =>
          throw new IllegalArgumentException(
            "data-center-replication-factors cannot be empty when using NetworkTopologyStrategy.")
        case dcrfs =>
          dcrfs.map { dataCenterWithReplicationFactor =>
            dataCenterWithReplicationFactor.split(":") match {
              case Array(dataCenter, replicationFactor) =>
                s"'$dataCenter':$replicationFactor"
              case msg =>
                throw new IllegalArgumentException(
                  s"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: $msg.")
            }
          }
      }
      result.mkString(",")
    }

    strategy.toLowerCase() match {
      case "simplestrategy" =>
        s"'SimpleStrategy','replication_factor':$replicationFactor"
      case "networktopologystrategy" =>
        s"'NetworkTopologyStrategy',${getDataCenterReplicationFactorList(dataCenterReplicationFactors)}"
      case unknownStrategy =>
        throw new IllegalArgumentException(s"$unknownStrategy as replication strategy is unknown and not supported.")
    }
  }

  /**
   * Validates that the supplied keyspace name is valid based on docs found here:
   * http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html
   *
   * @param keyspaceName - the keyspace name to validate.
   * @return - String if the keyspace name is valid, throws IllegalArgumentException otherwise.
   */
  def validateKeyspaceName(keyspaceName: String): String =
    if (keyspaceName.matches(keyspaceNameRegex)) {
      keyspaceName
    } else {
      throw new IllegalArgumentException(
        s"Invalid keyspace name. A keyspace may have 32 or fewer alpha-numeric characters and underscores. Value was: $keyspaceName")
    }

  /**
   * Validates that the supplied table name meets Cassandra's table name requirements.
   * According to docs here: https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt :
   *
   * @param tableName - the table name to validate
   * @return - String if the tableName is valid, throws an IllegalArgumentException otherwise.
   */
  def validateTableName(tableName: String): String =
    if (tableName.matches(keyspaceNameRegex)) {
      tableName
    } else {
      throw new IllegalArgumentException(
        s"Invalid table name. A table name may have 32 or fewer alpha-numeric characters and underscores. Value was: $tableName")
    }
}
