/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.JdkSSLOptions
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.QueryOptions
import com.datastax.driver.core.Session
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import com.typesafe.config.Config

/**
 * Default implementation of the `SessionProvider` that is used for creating the
 * Cassandra Session. This class is building the Cluster from configuration
 * properties.
 *
 * You may create a subclass of this that performs lookup the contact points
 * of the Cassandra cluster asynchronously instead of reading them in the
 * configuration. Such a subclass should override the [[#lookupContactPoints]]
 * method.
 *
 * The implementation is defined in configuration `session-provider` property.
 * The config parameter is the config section of the plugin.
 */
class ConfigSessionProvider(system: ActorSystem, config: Config) extends SessionProvider {

  def connect()(implicit ec: ExecutionContext): Future[Session] = {
    val clusterId = config.getString("cluster-id")
    clusterBuilder(clusterId).flatMap(_.build().connectAsync())
  }

  val fetchSize = config.getInt("max-result-size")
  val protocolVersion: Option[ProtocolVersion] = config.getString("protocol-version") match {
    case "" => None
    case _  => Some(ProtocolVersion.fromInt(config.getInt("protocol-version")))
  }

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

  val reconnectMaxDelay: FiniteDuration = config.getDuration("reconnect-max-delay", TimeUnit.MILLISECONDS).millis

  def clusterBuilder(clusterId: String)(implicit ec: ExecutionContext): Future[Cluster.Builder] = {
    lookupContactPoints(clusterId).map { cp =>
      val b = Cluster.builder
        .addContactPointsWithPorts(cp.asJava)
        .withPoolingOptions(poolingOptions)
        .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, reconnectMaxDelay.toMillis))
        .withQueryOptions(new QueryOptions().setFetchSize(fetchSize))
      protocolVersion match {
        case None    => b
        case Some(v) => b.withProtocolVersion(v)
      }

      val username = config.getString("authentication.username")
      if (username != "") {
        b.withCredentials(
          username,
          config.getString("authentication.password")
        )
      }

      val localDatacenter = config.getString("local-datacenter")
      if (localDatacenter != "") {
        val usedHostsPerRemoteDc = config.getInt("used-hosts-per-remote-dc")
        b.withLoadBalancingPolicy(
          new TokenAwarePolicy(
            DCAwareRoundRobinPolicy.builder
              .withLocalDc(localDatacenter)
              .withUsedHostsPerRemoteDc(usedHostsPerRemoteDc)
              .build()
          )
        )
      }

      val truststorePath = config.getString("ssl.truststore.path")
      if (truststorePath != "") {
        val trustStore = StorePathPasswordConfig(
          truststorePath,
          config.getString("ssl.truststore.password")
        )

        val keystorePath = config.getString("ssl.keystore.path")
        val keyStore: Option[StorePathPasswordConfig] =
          if (keystorePath != "") {
            val keyStore = StorePathPasswordConfig(
              keystorePath,
              config.getString("ssl.keystore.password")
            )
            Some(keyStore)
          } else None

        val context = SSLSetup.constructContext(trustStore, keyStore)

        b.withSSL(JdkSSLOptions.builder.withSSLContext(context).build())
      }

      b
    }
  }

  /**
   * Subclass may override this method to perform lookup the contact points
   * of the Cassandra cluster asynchronously instead of reading them from the
   * configuration.
   *
   * @param clusterId the configured `cluster-id` to lookup
   */
  def lookupContactPoints(clusterId: String)(implicit ec: ExecutionContext): Future[immutable.Seq[InetSocketAddress]] = {
    val port: Int = config.getInt("port")
    val contactPoints = config.getStringList("contact-points").asScala.toList
    Future.successful(buildContactPoints(contactPoints, port))
  }

  /**
   * Builds list of InetSocketAddress out of host:port pairs or host entries + given port parameter.
   */
  protected def buildContactPoints(contactPoints: immutable.Seq[String], port: Int): immutable.Seq[InetSocketAddress] = {
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
}
