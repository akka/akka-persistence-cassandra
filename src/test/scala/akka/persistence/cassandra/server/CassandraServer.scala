package akka.persistence.cassandra.server

import scala.concurrent.duration._

import org.cassandraunit.utils.EmbeddedCassandraServerHelper

object CassandraServer {
  def toggleSsl(path:String, toggle:Boolean):String = 
    if(toggle) path.dropRight(5) + "_ssl" + path.takeRight(5)
    else path

  def start(timeout: FiniteDuration = 10.seconds, withSsl:Boolean) = 
    CassandraServerHelper.startEmbeddedCassandra(
      toggleSsl("cassandra_network_strategy.yaml", withSsl), 
      timeout.toMillis)

  def clean() = EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
}
