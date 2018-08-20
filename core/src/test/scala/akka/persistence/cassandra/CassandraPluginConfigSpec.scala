/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.collection.immutable
import scala.concurrent.duration._
import java.net.InetSocketAddress
import com.typesafe.config.ConfigFactory
import org.scalatest.MustMatchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.util.Random
import org.scalatest.WordSpecLike
import akka.testkit.TestKit
import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.Future
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext
import org.scalatest.BeforeAndAfterAll

object CassandraPluginConfigSpec {
  class TestContactPointsProvider(system: ActorSystem, config: Config) extends ConfigSessionProvider(system, config) {
    override def lookupContactPoints(clusterId: String)(implicit ec: ExecutionContext): Future[immutable.Seq[InetSocketAddress]] = {
      if (clusterId == "cluster1")
        Future.successful(List(new InetSocketAddress("host1", 9041)))
      else
        Future.successful(List(new InetSocketAddress("host1", 9041), new InetSocketAddress("host2", 9042)))
    }

  }
}

class CassandraPluginConfigSpec extends TestKit(ActorSystem("CassandraPluginConfigSpec"))
  with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  import CassandraPluginConfigSpec._
  import system.dispatcher
  lazy val defaultConfig = ConfigFactory.load().getConfig("cassandra-journal")

  lazy val keyspaceNames = {
    // Generate a key that is the max acceptable length ensuring the first char is alpha
    def maxKey = Random.alphanumeric.dropWhile(_.toString.matches("[^a-zA-Z]")).take(48).mkString

    Table(
      ("Keyspace", "isValid"),
      ("test", true),
      ("_test_123", false),
      ("", false),
      ("test-space", false),
      ("'test'", false),
      ("a", true),
      ("a_", true),
      ("1", false),
      ("a1", true),
      ("_", false),
      ("asdf!", false),
      (maxKey, true),
      ("\"_asdf\"", false),
      ("\"_\"", false),
      ("\"a\"", true),
      ("\"a_sdf\"", true),
      ("\"\"", false),
      ("\"valid_with_quotes\"", true),
      ("\"missing_trailing_quote", false),
      ("missing_leading_quote\"", false),
      ('"' + maxKey + '"', true),
      (maxKey + "_", false))
  }

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    super.afterAll()
  }

  "A CassandraPluginConfig" should {
    "use ConfigSessionProvider by default" in {
      val config = new CassandraPluginConfig(system, defaultConfig)
      config.sessionProvider.getClass must be(classOf[ConfigSessionProvider])
    }

    "set the fetch size to the max result size" in {
      val config = new CassandraPluginConfig(system, defaultConfig)
      config.sessionProvider.asInstanceOf[ConfigSessionProvider].fetchSize must be(250)
    }

    "set the metadata table" in {
      val config = new CassandraPluginConfig(system, defaultConfig)
      config.metadataTable must be("metadata")
    }

    "parse config with host:port values as contact points" in {
      val configWithHostPortPair = ConfigFactory.parseString("""contact-points = ["127.0.0.1:19142", "127.0.0.1:29142"]""").withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithHostPortPair)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints(""), 3.seconds) must be(
        List(
          new InetSocketAddress("127.0.0.1", 19142),
          new InetSocketAddress("127.0.0.1", 29142)))
    }

    "ignore the port configuration with host:port values as contact points" in {
      val configWithHostPortPairAndPort = ConfigFactory.parseString(
        """
          contact-points = ["127.0.0.1:19142", "127.0.0.1:29142", "127.0.0.1"]
          port = 39142
        """).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithHostPortPairAndPort)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints(""), 3.seconds) must be(
        List(
          new InetSocketAddress("127.0.0.1", 19142),
          new InetSocketAddress("127.0.0.1", 29142),
          new InetSocketAddress("127.0.0.1", 39142)))
    }

    "parse config with a list of contact points without port" in {
      lazy val configWithHosts = ConfigFactory.parseString("""contact-points = ["127.0.0.1", "127.0.0.2"]""").withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithHosts)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints(""), 3.seconds) must be(
        List(
          new InetSocketAddress("127.0.0.1", 9042),
          new InetSocketAddress("127.0.0.2", 9042)))
    }

    "use the port configuration with a list of contact points without port" in {
      lazy val configWithHostsAndPort = ConfigFactory.parseString(
        """
          contact-points = ["127.0.0.1", "127.0.0.2"]
          port = 19042
        """).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithHostsAndPort)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints(""), 3.seconds) must be(
        List(
          new InetSocketAddress("127.0.0.1", 19042),
          new InetSocketAddress("127.0.0.2", 19042)))
    }

    "set the port configuration on the cluster builder" in {
      lazy val configWithHostsAndPort = ConfigFactory.parseString(
        """
          contact-points = ["127.0.0.1", "127.0.0.2"]
          port = 19042
        """).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithHostsAndPort)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      val clusterBuilder = Await.result(sessionProvider.clusterBuilder(""), 3.seconds)
      clusterBuilder.getConfiguration.getProtocolOptions.getPort mustBe 19042
    }

    "use custom ConfigSessionProvider for cluster1" in {
      val configWithContactPointsProvider = ConfigFactory.parseString(s"""
        session-provider = "${classOf[TestContactPointsProvider].getName}"
        cluster-id = cluster1
      """).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithContactPointsProvider)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints("cluster1"), 3.seconds) must be(
        List(new InetSocketAddress("host1", 9041)))
    }

    "use custom ConfigSessionProvider for cluster2" in {
      val configWithContactPointsProvider = ConfigFactory.parseString(s"""
        session-provider = "${classOf[TestContactPointsProvider].getName}"
        cluster-id = cluster2
      """).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithContactPointsProvider)
      val sessionProvider = config.sessionProvider.asInstanceOf[ConfigSessionProvider]
      Await.result(sessionProvider.lookupContactPoints("cluster2"), 3.seconds) must be(
        List(new InetSocketAddress("host1", 9041), new InetSocketAddress("host2", 9042)))
    }

    "throw an exception when contact point list is empty" in {
      val cfg = ConfigFactory.parseString("""contact-points = []""").withFallback(defaultConfig)
      intercept[IllegalArgumentException] {
        val config = new CassandraPluginConfig(system, cfg)
        Await.result(config.sessionProvider.connect(), 3.seconds)
      }
    }

    "parse config with SimpleStrategy as default for replication-strategy" in {
      val config = new CassandraPluginConfig(system, defaultConfig)
      config.replicationStrategy must be("'SimpleStrategy','replication_factor':1")
    }

    "parse config with a list of datacenters configured for NetworkTopologyStrategy" in {
      lazy val configWithNetworkStrategy = ConfigFactory.parseString(
        """
          |replication-strategy = "NetworkTopologyStrategy"
          |data-center-replication-factors = ["dc1:3", "dc2:2"]
        """.stripMargin).withFallback(defaultConfig)
      val config = new CassandraPluginConfig(system, configWithNetworkStrategy)
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
        if (isValid) CassandraPluginConfig.validateKeyspaceName(keyspace) must be(keyspace)
        else intercept[IllegalArgumentException] {
          CassandraPluginConfig.validateKeyspaceName(keyspace)
        }
      }
    }

    "validate table name parameter" in {
      forAll(keyspaceNames) { (tableName, isValid) =>
        if (isValid) CassandraPluginConfig.validateKeyspaceName(tableName) must be(tableName)
        else intercept[IllegalArgumentException] {
          CassandraPluginConfig.validateKeyspaceName(tableName)
        }
      }
    }

    "parse keyspace-autocreate parameter" in {
      val configWithFalseKeyspaceAutocreate = ConfigFactory.parseString("""keyspace-autocreate = false""").withFallback(defaultConfig)

      val config = new CassandraPluginConfig(system, configWithFalseKeyspaceAutocreate)
      config.keyspaceAutoCreate must be(false)
    }

    "parse tables-autocreate parameter" in {
      val configWithFalseTablesAutocreate = ConfigFactory.parseString("""tables-autocreate = false""").withFallback(defaultConfig)

      val config = new CassandraPluginConfig(system, configWithFalseTablesAutocreate)
      config.tablesAutoCreate must be(false)
    }
  }
}
