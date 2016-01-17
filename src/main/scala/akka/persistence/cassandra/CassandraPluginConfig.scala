/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import com.datastax.driver.core.policies.{ TokenAwarePolicy, DCAwareRoundRobinPolicy }
import com.datastax.driver.core._
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CassandraPluginConfig(config: Config) {

  import akka.persistence.cassandra.CassandraPluginConfig._

  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")
  val metadataTable: String = config.getString("metadata-table")
  val configTable: String = validateTableName(config.getString("config-table"))

  val tableCompactionStrategy: CassandraCompactionStrategy = CassandraCompactionStrategy(config.getConfig("table-compaction-strategy"))

  val keyspaceAutoCreate: Boolean = config.getBoolean("keyspace-autocreate")

  val connectionRetries: Int = config.getInt("connect-retries")
  val connectionRetryDelay: FiniteDuration = config.getDuration("connect-retry-delay", TimeUnit.MILLISECONDS).millis

  val replicationStrategy: String = getReplicationStrategy(
    config.getString("replication-strategy"),
    config.getInt("replication-factor"),
    config.getStringList("data-center-replication-factors").asScala
  )

  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val port: Int = config.getInt("port")
  val contactPoints = getContactPoints(config.getStringList("contact-points").asScala, port)
  val fetchSize = config.getInt("max-result-size")

  private[this] val connectionPoolConfig = config.getConfig("connection-pool")

  val poolingOptions = new PoolingOptions()
    .setNewConnectionThreshold(
      HostDistance.LOCAL,
      connectionPoolConfig.getInt("new-connection-threshold-local")
    )
    .setNewConnectionThreshold(
      HostDistance.REMOTE,
      connectionPoolConfig.getInt("new-connection-threshold-remote")
    )
    .setMaxRequestsPerConnection(
      HostDistance.LOCAL,
      connectionPoolConfig.getInt("max-requests-per-connection-local")
    )
    .setMaxRequestsPerConnection(
      HostDistance.REMOTE,
      connectionPoolConfig.getInt("max-requests-per-connection-remote")
    )
    .setConnectionsPerHost(
      HostDistance.LOCAL,
      connectionPoolConfig.getInt("connections-per-host-core-local"),
      connectionPoolConfig.getInt("connections-per-host-max-local")
    )
    .setConnectionsPerHost(
      HostDistance.REMOTE,
      connectionPoolConfig.getInt("connections-per-host-core-remote"),
      connectionPoolConfig.getInt("connections-per-host-max-remote")
    )
    .setPoolTimeoutMillis(
      connectionPoolConfig.getInt("pool-timeout-millis")
    )

  val clusterBuilder: Cluster.Builder = Cluster.builder
    .addContactPointsWithPorts(contactPoints.asJava)
    .withPoolingOptions(poolingOptions)
    .withQueryOptions(new QueryOptions().setFetchSize(fetchSize))

  if (config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password")
    )
  }

  if (config.hasPath("local-datacenter")) {
    clusterBuilder.withLoadBalancingPolicy(
      new TokenAwarePolicy(
        DCAwareRoundRobinPolicy.builder.withLocalDc(config.getString("local-datacenter")).build()
      )
    )
  }

  if (config.hasPath("ssl")) {
    val trustStorePath: String = config.getString("ssl.truststore.path")
    val trustStorePW: String = config.getString("ssl.truststore.password")
    val keyStorePath: String = config.getString("ssl.keystore.path")
    val keyStorePW: String = config.getString("ssl.keystore.password")

    val context = SSLSetup.constructContext(
      trustStorePath,
      trustStorePW,
      keyStorePath,
      keyStorePW
    )

    // FIXME there is also a NettySSLOptions? what about SSLOptions?
    clusterBuilder.withSSL(JdkSSLOptions.builder.withSSLContext(context).build())
  }
}

object CassandraPluginConfig {

  val keyspaceNameRegex = """^("[a-zA-Z]{1}[\w]{0,47}"|[a-zA-Z]{1}[\w]{0,47})$"""

  /**
   * Builds list of InetSocketAddress out of host:port pairs or host entries + given port parameter.
   */
  def getContactPoints(contactPoints: Seq[String], port: Int): Seq[InetSocketAddress] = {
    contactPoints match {
      case null | Nil => throw new IllegalArgumentException("A contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort =>
          ipWithPort.split(":") match {
            case Array(host, port) => new InetSocketAddress(host, port.toInt)
            case Array(host)       => new InetSocketAddress(host, port)
            case msg               => throw new IllegalArgumentException(s"A contact point should have the form [host:port] or [host] but was: $msg.")
          }
      }
    }
  }

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
