package akka.persistence.cassandra.server

import scala.concurrent.duration._

import org.cassandraunit.utils.EmbeddedCassandraServerHelper

object CassandraServer {
  def start(timeout: FiniteDuration = 10.seconds) = CassandraServerHelper.startEmbeddedCassandra("cassandra_network_strategy.yaml", timeout.toMillis)

  def clean() = EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
}
