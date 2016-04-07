/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import scala.collection.immutable
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import com.datastax.driver.core.policies.{ TokenAwarePolicy, DCAwareRoundRobinPolicy }
import com.datastax.driver.core._
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import scala.util.Failure
import scala.concurrent.Future

case class StorePathPasswordConfig(path: String, password: String)

class CassandraPluginConfig(system: ActorSystem, config: Config) {

  import akka.persistence.cassandra.CassandraPluginConfig._

  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")
  val metadataTable: String = config.getString("metadata-table")
  val configTable: String = validateTableName(config.getString("config-table"))

  val tableCompactionStrategy: CassandraCompactionStrategy = CassandraCompactionStrategy(config.getConfig("table-compaction-strategy"))

  val keyspaceAutoCreate: Boolean = config.getBoolean("keyspace-autocreate")
  val tablesAutoCreate: Boolean = config.getBoolean("tables-autocreate")

  val connectionRetries: Int = config.getInt("connect-retries")
  val connectionRetryDelay: FiniteDuration = config.getDuration("connect-retry-delay", TimeUnit.MILLISECONDS).millis

  val replicationStrategy: String = getReplicationStrategy(
    config.getString("replication-strategy"),
    config.getInt("replication-factor"),
    config.getStringList("data-center-replication-factors").asScala
  )

  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))

  val blockingDispatcherId: String = config.getString("blocking-dispatcher")

  val sessionProvider: SessionProvider = {
    val className = config.getString("session-provider")
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val clazz = dynamicAccess.getClassFor[SessionProvider](className).get

    def instantiate(args: immutable.Seq[(Class[_], AnyRef)]) =
      dynamicAccess.createInstanceFor[SessionProvider](clazz, args)

    instantiate(List((classOf[ActorSystem], system), (classOf[Config], config)))
      .recoverWith { case x: NoSuchMethodException ⇒ instantiate(List((classOf[ActorSystem], system))) }
      .recoverWith { case x: NoSuchMethodException ⇒ instantiate(Nil) }
      .recoverWith {
        case ex: Exception ⇒
          Failure(new IllegalArgumentException(s"Unable to create SessionProvider instance for class [$className]", ex))
      }.get
  }

  // FIXME temporary until we have fixed all blocking
  private[cassandra] val blockingTimeout: FiniteDuration = 10.seconds
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
   *   http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html
   * @param keyspaceName - the keyspace name to validate.
   * @return - String if the keyspace name is valid, throws IllegalArgumentException otherwise.
   */
  def validateKeyspaceName(keyspaceName: String): String = keyspaceName.matches(keyspaceNameRegex) match {
    case true  => keyspaceName
    case false => throw new IllegalArgumentException(s"Invalid keyspace name. A keyspace may 32 or fewer alpha-numeric characters and underscores. Value was: $keyspaceName")
  }

  /**
   * Validates that the supplied table name meets Cassandra's table name requirements.
   * According to docs here: https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt :
   *
   * @param tableName - the table name to validate
   * @return - String if the tableName is valid, throws an IllegalArgumentException otherwise.
   */
  def validateTableName(tableName: String): String = tableName.matches(keyspaceNameRegex) match {
    case true  => tableName
    case false => throw new IllegalArgumentException(s"Invalid table name. A table name may 32 or fewer alpha-numeric characters and underscores. Value was: $tableName")
  }
}
