/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import com.datastax.driver.core._
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.persistence.cassandra.session.CassandraSessionSettings

case class StorePathPasswordConfig(path: String, password: String)

class CassandraPluginConfig(system: ActorSystem, config: Config) {

  import akka.persistence.cassandra.CassandraPluginConfig._

  val sessionProvider: SessionProvider =
    SessionProvider(system.asInstanceOf[ExtendedActorSystem], config)

  val sessionSettings: CassandraSessionSettings =
    CassandraSessionSettings(config)

  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")
  val metadataTable: String = config.getString("metadata-table")
  val configTable: String = validateTableName(config.getString("config-table"))

  val tableCompactionStrategy: CassandraCompactionStrategy = CassandraCompactionStrategy(config.getConfig("table-compaction-strategy"))

  val keyspaceAutoCreate: Boolean = config.getBoolean("keyspace-autocreate")
  val tablesAutoCreate: Boolean = config.getBoolean("tables-autocreate")

  val replicationStrategy: String = getReplicationStrategy(
    config.getString("replication-strategy"),
    config.getInt("replication-factor"),
    config.getStringList("data-center-replication-factors").asScala)

  val readConsistency: ConsistencyLevel = sessionSettings.readConsistency
  val writeConsistency: ConsistencyLevel = sessionSettings.writeConsistency

  val deleteRetries: Int = config.getInt("delete-retries")
  val writeRetries: Int = config.getInt("write-retries")
  val readRetries: Int = config.getInt("read-retries")

  val gcGraceSeconds: Long = config.getLong("gc-grace-seconds")

}

object CassandraPluginConfig {

  val keyspaceNameRegex = """^("[a-zA-Z]{1}[\w]{0,47}"|[a-zA-Z]{1}[\w]{0,47})$"""

  /**
   * Builds replication strategy command to create a keyspace.
   */
  def getReplicationStrategy(strategy: String, replicationFactor: Int, dataCenterReplicationFactors: Seq[String]): String = {

    def getDataCenterReplicationFactorList(dcrfList: Seq[String]): String = {
      val result: Seq[String] = dcrfList match {
        case null | Nil => throw new IllegalArgumentException("data-center-replication-factors cannot be empty when using NetworkTopologyStrategy.")
        case dcrfs => dcrfs.map {
          dataCenterWithReplicationFactor =>
            dataCenterWithReplicationFactor.split(":") match {
              case Array(dataCenter, replicationFactor) => s"'$dataCenter':$replicationFactor"
              case msg                                  => throw new IllegalArgumentException(s"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: $msg.")
            }
        }
      }
      result.mkString(",")
    }

    strategy.toLowerCase() match {
      case "simplestrategy"          => s"'SimpleStrategy','replication_factor':$replicationFactor"
      case "networktopologystrategy" => s"'NetworkTopologyStrategy',${getDataCenterReplicationFactorList(dataCenterReplicationFactors)}"
      case unknownStrategy           => throw new IllegalArgumentException(s"$unknownStrategy as replication strategy is unknown and not supported.")
    }
  }

  /**
   * Validates that the supplied keyspace name is valid based on docs found here:
   * http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html
   *
   * @param keyspaceName - the keyspace name to validate.
   * @return - String if the keyspace name is valid, throws IllegalArgumentException otherwise.
   */
  def validateKeyspaceName(keyspaceName: String): String = if (keyspaceName.matches(keyspaceNameRegex)) {
    keyspaceName
  } else {
    throw new IllegalArgumentException(s"Invalid keyspace name. A keyspace may have 32 or fewer alpha-numeric characters and underscores. Value was: $keyspaceName")
  }

  /**
   * Validates that the supplied table name meets Cassandra's table name requirements.
   * According to docs here: https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt :
   *
   * @param tableName - the table name to validate
   * @return - String if the tableName is valid, throws an IllegalArgumentException otherwise.
   */
  def validateTableName(tableName: String): String = if (tableName.matches(keyspaceNameRegex)) {
    tableName
  } else {
    throw new IllegalArgumentException(s"Invalid table name. A table name may have 32 or fewer alpha-numeric characters and underscores. Value was: $tableName")
  }
}
