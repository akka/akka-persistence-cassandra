package akka.persistence.cassandra

import java.net.InetSocketAddress

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.{Cluster, ConsistencyLevel}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class CassandraPluginConfig(config: Config) {

  import akka.persistence.cassandra.CassandraPluginConfig._

  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")

  val replicationFactor: Int = config.getInt("replication-factor")
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val port: Int = config.getInt("port")
  val contactPoints = getContactPoints(config.getStringList("contact-points").asScala, port)

  val clusterBuilder: Cluster.Builder = Cluster.builder
    .addContactPointsWithPorts(contactPoints.asJava)

  if (config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password"))
  }

  if (config.hasPath("local-datacenter")) {
    clusterBuilder.withLoadBalancingPolicy(
      new DCAwareRoundRobinPolicy(config.getString("local-datacenter"))
    )
  }
}

object CassandraPluginConfig {

  /**
   * Builds list of InetSocketAddress out of host:port pairs or host entries + given port parameter.
   */
  def getContactPoints(contactPoints: Seq[String], port: Int): Seq[InetSocketAddress] = {
    contactPoints match {
      case null | Nil => throw new IllegalArgumentException("A contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort => ipWithPort.split(":") match {
          case Array(host, port) => new InetSocketAddress(host, port.toInt)
          case Array(host) => new InetSocketAddress(host, port)
          case msg => throw new IllegalArgumentException(s"A contact point should have the form [host:port] or [host] but was: $msg.")
        }
      }
    }
  }
}
