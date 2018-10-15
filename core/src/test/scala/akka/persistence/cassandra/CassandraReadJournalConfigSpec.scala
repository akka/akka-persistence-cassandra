/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Day, Hour, TimeBucket }
import akka.persistence.cassandra.query.CassandraReadJournalConfig
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class CassandraReadJournalConfigSpec extends TestKit(ActorSystem("CassandraReadJournalConfigSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override protected def afterAll(): Unit = shutdown()

  "Cassandra read journal config" must {
    "support Day with just day format" in {
      val config = ConfigFactory.parseString(
        """
          |cassandra-journal.events-by-tag.bucket-size = Day
          |cassandra-query-journal.first-time-bucket = "20151120"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Day)
    }

    "support Day with full time format" in {
      val config = ConfigFactory.parseString(
        """
          |cassandra-journal.events-by-tag.bucket-size = Day
          |cassandra-query-journal.first-time-bucket = "20151120T12:20"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(config.getConfig("cassandra-query-journal"), writeConfig)

      // Rounded down
      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Day)
    }

    "support Hour with just hour format" in {
      val config = ConfigFactory.parseString(
        """
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "20151120T00"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Hour)
    }

    "support Hour with full time format" in {
      val config = ConfigFactory.parseString(
        """
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "20151120T00:20"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Hour)
    }

    "validate format" in {
      val config = ConfigFactory.parseString(
        """
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "cats"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val e = intercept[IllegalArgumentException] {
        new CassandraReadJournalConfig(config.getConfig("cassandra-query-journal"), writeConfig)
      }
      e.getMessage shouldEqual "Invalid first-time-bucket format. Use: yyyyMMdd'T'HH:mm"
    }
  }
}
