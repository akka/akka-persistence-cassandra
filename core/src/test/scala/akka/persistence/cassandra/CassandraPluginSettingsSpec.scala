/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.util.Random

import akka.persistence.cassandra.journal.JournalSettings

class CassandraPluginSettingsSpec
    extends TestKit(ActorSystem("CassandraPluginConfigSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  lazy val defaultConfig = ConfigFactory.load().getConfig("akka.persistence.cassandra")

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
      ('"'.toString + maxKey + '"'.toString, true), // using interpolation here breaks scalafmt :-/
      (maxKey + "_", false))
  }

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    super.afterAll()
  }

  "A CassandraJournalSettings" must {

    "set the metadata table" in {
      val config = new JournalSettings(system, defaultConfig)
      config.metadataTable must be("metadata")
    }

    "parse config with SimpleStrategy as default for replication-strategy" in {
      val config = new JournalSettings(system, defaultConfig)
      config.replicationStrategy must be("'SimpleStrategy','replication_factor':1")
    }

    "parse config with a list of datacenters configured for NetworkTopologyStrategy" in {
      lazy val configWithNetworkStrategy =
        ConfigFactory.parseString("""
          |journal.replication-strategy = "NetworkTopologyStrategy"
          |journal.data-center-replication-factors = ["dc1:3", "dc2:2"]
        """.stripMargin).withFallback(defaultConfig)
      val config = new JournalSettings(system, configWithNetworkStrategy)
      config.replicationStrategy must be("'NetworkTopologyStrategy','dc1':3,'dc2':2")
    }

    "parse config with a list of datacenters configured for NetworkTopologyStrategy using dot syntax" in {
      lazy val configWithNetworkStrategy =
        ConfigFactory.parseString("""
          |journal.replication-strategy = "NetworkTopologyStrategy"
          |journal.data-center-replication-factors.0 = "dc1:3"
          |journal.data-center-replication-factors.1 = "dc2:2"
        """.stripMargin).withFallback(defaultConfig)
      val config = new JournalSettings(system, configWithNetworkStrategy)
      config.replicationStrategy must be("'NetworkTopologyStrategy','dc1':3,'dc2':2")
    }

    "parse config with comma-separated data-center-replication-factors" in {
      lazy val configWithNetworkStrategy =
        ConfigFactory.parseString("""
          |journal.replication-strategy = "NetworkTopologyStrategy"
          |journal.data-center-replication-factors = "dc1:3,dc2:2"
        """.stripMargin).withFallback(defaultConfig)
      val config = new JournalSettings(system, configWithNetworkStrategy)
      config.replicationStrategy must be("'NetworkTopologyStrategy','dc1':3,'dc2':2")
    }

    "throw an exception for an unknown replication strategy" in {
      intercept[IllegalArgumentException] {
        PluginSettings.getReplicationStrategy("UnknownStrategy", 0, List.empty)
      }
    }

    "throw an exception when data-center-replication-factors is invalid or empty for NetworkTopologyStrategy" in {
      intercept[IllegalArgumentException] {
        PluginSettings.getReplicationStrategy("NetworkTopologyStrategy", 0, List.empty)
      }
      intercept[IllegalArgumentException] {
        PluginSettings.getReplicationStrategy("NetworkTopologyStrategy", 0, null)
      }
      intercept[IllegalArgumentException] {
        PluginSettings.getReplicationStrategy("NetworkTopologyStrategy", 0, Seq("dc1"))
      }
    }

    "validate keyspace parameter" in {
      forAll(keyspaceNames) { (keyspace, isValid) =>
        if (isValid) PluginSettings.validateKeyspaceName(keyspace) must be(keyspace)
        else
          intercept[IllegalArgumentException] {
            PluginSettings.validateKeyspaceName(keyspace)
          }
      }
    }

    "validate table name parameter" in {
      forAll(keyspaceNames) { (tableName, isValid) =>
        if (isValid) PluginSettings.validateKeyspaceName(tableName) must be(tableName)
        else
          intercept[IllegalArgumentException] {
            PluginSettings.validateKeyspaceName(tableName)
          }
      }
    }

    "parse keyspace-autocreate parameter" in {
      val configWithFalseKeyspaceAutocreate =
        ConfigFactory.parseString("journal.keyspace-autocreate = false").withFallback(defaultConfig)

      val config = new JournalSettings(system, configWithFalseKeyspaceAutocreate)
      config.keyspaceAutoCreate must be(false)
    }

    "parse tables-autocreate parameter" in {
      val configWithFalseTablesAutocreate =
        ConfigFactory.parseString("journal.tables-autocreate = false").withFallback(defaultConfig)

      val config = new JournalSettings(system, configWithFalseTablesAutocreate)
      config.tablesAutoCreate must be(false)
    }
  }

}
