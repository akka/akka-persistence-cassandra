package akka.persistence.cassandra

import scala.collection.JavaConverters._

import com.typesafe.config.Config
import com.datastax.driver.core.{ConsistencyLevel, Cluster}
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy

class CassandraPluginConfig(config: Config) {
  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")

  val replicationFactor: Int = config.getInt("replication-factor")
  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))

  val clusterBuilder: Cluster.Builder = Cluster.builder
    .addContactPoints(config.getStringList("contact-points").asScala: _*)
    .withPort(config.getInt("port"))

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
