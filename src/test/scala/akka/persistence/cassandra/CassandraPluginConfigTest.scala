package akka.persistence.cassandra

import java.net.InetSocketAddress

import com.typesafe.config.ConfigFactory
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.Random

/**
 *
 */
class CassandraPluginConfigTest extends WordSpec with MustMatchers {
  lazy val defaultConfig = ConfigFactory.parseString(
    """
      |keyspace-autocreate = true
      |keyspace-autocreate-retries = 1
      |keyspace = test-keyspace
      |connect-retries = 3
      |connect-retry-delay = 5s
      |table = test-table
      |table-compaction-strategy { class = "SizeTieredCompactionStrategy" }
      |metadata-table = test-metadata-table
      |config-table = config
      |replication-strategy = "SimpleStrategy"
      |replication-factor = 1
      |data-center-replication-factors = []
      |read-consistency = QUORUM
      |write-consistency = QUORUM
      |contact-points = ["127.0.0.1"]
      |port = 9142
      |max-result-size = 50
      |delete-retries = 4
    """.stripMargin)


  lazy val keyspaceNames = {
    // Generate a key that is the max acceptable length ensuring the first char is alpha
    def maxKey = Random.alphanumeric.dropWhile(_.toString.matches("[^a-zA-Z]")).take(32).mkString

    Table (
      ("Keyspace", "isValid"),
      ("test",        true),
      ("_test_123",   false),
      ("",            false),
      ("test-space",  false),
      ("'test'",      false),
      ("a",           true),
      ("a_",          true),
      ("1",           false),
      ("a1",          true),
      ("_",           false),
      ("asdf!",       false),
      (maxKey,        true),
      ("\"_asdf\"",   false),
      ("\"_\"",       false),
      ("\"a\"",       true),
      ("\"a_sdf\"",   true),
      ("\"\"",        false),
      ("\"valid_with_quotes\"",       true),
      ("\"missing_trailing_quote",    false),
      ("missing_leading_quote\"",     false),
      ('"' + maxKey + '"',            true),
      (maxKey + "_",                  false)
    )
  }


  "A CassandraPluginConfig" should {
    "set the fetch size to the max result size" in {
      val config = new CassandraPluginConfig(defaultConfig)
      config.fetchSize must be(50)
    }

    "set the metadata table" in {
      val config = new CassandraPluginConfig(defaultConfig)
      config.metadataTable must be("test-metadata-table")
    }

    "parse config with host:port values as contact points" in {
      val configWithHostPortPair = ConfigFactory.parseString( """contact-points = ["127.0.0.1:19142", "127.0.0.1:29142"]""").withFallback(defaultConfig)
      val config = new CassandraPluginConfig(configWithHostPortPair)
      config.contactPoints must be(
        List(
          new InetSocketAddress("127.0.0.1", 19142),
          new InetSocketAddress("127.0.0.1", 29142)
        )
      )

    }

    "parse config with a list of contact points without port" in {
      lazy val configWithHosts = ConfigFactory.parseString( """contact-points = ["127.0.0.1", "127.0.0.2"]""").withFallback(defaultConfig)
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

    "parse config with SimpleStrategy as default for replication-strategy" in {
      val config = new CassandraPluginConfig(defaultConfig)
      config.replicationStrategy must be("'SimpleStrategy','replication_factor':1")
    }

    "parse config with a list of datacenters configured for NetworkTopologyStrategy" in {
      lazy val configWithNetworkStrategy = ConfigFactory.parseString(
        """
          |replication-strategy = "NetworkTopologyStrategy"
          |data-center-replication-factors = ["dc1:3", "dc2:2"]
        """.stripMargin).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(configWithNetworkStrategy)
      config.replicationStrategy must be("'NetworkTopologyStrategy','dc1':3,'dc2':2")
    }

    "throw an exception for an unknown replication strategy" in {
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getReplicationStrategy("UnknownStrategy", 0, List.empty)
      }
    }

    "throw an exception when data-center-replication-factors is invalid or empty for NetworkTopologyStrategy" in {
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getReplicationStrategy("NetworkTopologyStrategy", 0, List.empty)
      }
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getReplicationStrategy("NetworkTopologyStrategy", 0, null)
      }
      intercept[IllegalArgumentException] {
        CassandraPluginConfig.getReplicationStrategy("NetworkTopologyStrategy", 0, Seq("dc1"))
      }
    }

    "validate keyspace parameter" in {
      forAll(keyspaceNames) { (keyspace, isValid) =>
        if(isValid) CassandraPluginConfig.validateKeyspaceName(keyspace) must be(keyspace)
        else intercept[IllegalArgumentException] {
          CassandraPluginConfig.validateKeyspaceName(keyspace)
        }
      }
    }

    "validate table name parameter" in {
      forAll(keyspaceNames) { (tableName, isValid) =>
        if(isValid) CassandraPluginConfig.validateKeyspaceName(tableName) must be(tableName)
        else intercept[IllegalArgumentException] {
          CassandraPluginConfig.validateKeyspaceName(tableName)
        }
      }
    }


    "parse keyspace-autocreate parameter" in {
      val configWithFalseKeyspaceAutocreate = ConfigFactory.parseString( """keyspace-autocreate = false""").withFallback(defaultConfig)

      val config = new CassandraPluginConfig(configWithFalseKeyspaceAutocreate)
      config.keyspaceAutoCreate must be(false)
    }
  }
}
