package akka.persistence.cassandra

import java.net.InetSocketAddress

import com.typesafe.config.ConfigFactory
import org.scalatest.{MustMatchers, Matchers, WordSpec}

/**
 *
 */
class CassandraPluginConfigTest extends WordSpec with MustMatchers {
  lazy val configWithHostPortPair = ConfigFactory.parseString(
    """
      |
      |keyspace = test-keyspace
      |table = test-table
      |replication-factor = 1
      |read-consistency = QUORUM
      |write-consistency = QUORUM
      |contact-points = ["127.0.0.1:19142", "127.0.0.1:29142"]
      |port = 9142
    """.stripMargin)

  lazy val configWithHosts = ConfigFactory.parseString(
    """
      |
      |keyspace = test-keyspace
      |table = test-table
      |replication-factor = 1
      |read-consistency = QUORUM
      |write-consistency = QUORUM
      |contact-points = ["127.0.0.1", "127.0.0.2"]
      |port = 9142
    """.stripMargin)

  "A CassandraPluginConfig" should {
    "parse config with host:port values as contact points" in {
      val config = new CassandraPluginConfig(configWithHostPortPair)
      config.contactPoints must be(
        List(
          new InetSocketAddress("127.0.0.1", 19142),
          new InetSocketAddress("127.0.0.1", 29142)
        )
      )

    }
    "parse config with a list of contact points without port" in {
      val config = new CassandraPluginConfig(configWithHosts)
      config.contactPoints must be(
        List(
          new InetSocketAddress("127.0.0.1", 9142),
          new InetSocketAddress("127.0.0.2", 9142)
        )
      )
    }
    "throw an exception when contact point list is empty" in {
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getContactPoints(List.empty, 0)
      }
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getContactPoints(null, 0)
      }
    }
  }
}
