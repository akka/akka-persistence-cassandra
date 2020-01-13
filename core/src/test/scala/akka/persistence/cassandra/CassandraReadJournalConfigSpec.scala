/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Day, Hour, TimeBucket }
import akka.persistence.cassandra.query.CassandraReadJournalConfig
import akka.persistence.cassandra.query.CassandraReadJournalConfig.BackTrackConfig
import akka.persistence.cassandra.query.CassandraReadJournalConfig.Fixed
import akka.persistence.cassandra.query.CassandraReadJournalConfig.Max
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class CassandraReadJournalConfigSpec
    extends TestKit(ActorSystem("CassandraReadJournalConfigSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = shutdown()

  "Cassandra read journal config" must {

    "default persistence id cleanup to 2x bucket" in {
      import scala.concurrent.duration._
      val config = ConfigFactory.parseString("""
         cassandra-journal.events-by-tag.bucket-size = Hour
        """).withFallback(system.settings.config)

      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.eventsByTagCleanUpPersistenceIds.get shouldEqual 2.hours
    }

    "support Day with just day format" in {
      val config = ConfigFactory.parseString("""
          |cassandra-journal.events-by-tag.bucket-size = Day
          |cassandra-query-journal.first-time-bucket = "20151120"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Day)
    }

    "support Day with full time format" in {
      val config = ConfigFactory.parseString("""
          |cassandra-journal.events-by-tag.bucket-size = Day
          |cassandra-query-journal.first-time-bucket = "20151120T12:20"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)

      // Rounded down
      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Day)
    }

    "support Hour with just hour format" in {
      val config = ConfigFactory.parseString("""
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "20151120T00"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Hour)
    }

    "support Hour with full time format" in {
      val config = ConfigFactory.parseString("""
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "20151120T00:20"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val readConfig = new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)

      readConfig.firstTimeBucket shouldEqual TimeBucket(1447977600000L, Hour)
    }

    "validate format" in {
      val config = ConfigFactory.parseString("""
          |cassandra-journal.events-by-tag.bucket-size = Hour
          |cassandra-query-journal.first-time-bucket = "cats"
        """.stripMargin).withFallback(system.settings.config)
      val writeConfig = new CassandraJournalConfig(system, config.getConfig("cassandra-journal"))
      val e = intercept[IllegalArgumentException] {
        new CassandraReadJournalConfig(system, config.getConfig("cassandra-query-journal"), writeConfig)
      }
      e.getMessage shouldEqual "Invalid first-time-bucket format. Use: yyyyMMdd'T'HH:mm"
    }
  }
}

class BackTrackConfigSpec extends WordSpec with Matchers {
  import scala.concurrent.duration._
  val currentTime = 100
  "BackTrack config" should {

    val baseConfig = BackTrackConfig(None, None, Max, None, Max)

    "set long interval to max mills if disabled" in {
      val longIntervalDisabled = baseConfig.copy(longInterval = None)
      longIntervalDisabled.longIntervalMillis() shouldEqual Long.MaxValue
    }
    "set interval to max mills if disabled" in {
      baseConfig.copy(interval = None).intervalMillis() shouldEqual Long.MaxValue
    }
    "set long interval to mills if set" in {
      baseConfig.copy(interval = Some(1.millis), longInterval = Some(10.millis)).longIntervalMillis() shouldEqual 10
    }
    "set interval to mills if set" in {
      baseConfig.copy(interval = Some(1.millis), longInterval = Some(10.millis)).intervalMillis() shouldEqual 1
    }

    "cap period at metadataCleanupInterval for max long period" in {
      baseConfig
        .copy(metadataCleanupInterval = Some(20.millis), period = Max)
        .periodMillis(currentTime, 0) shouldEqual (currentTime - 20)
    }
    "use fixed period if set" in {
      baseConfig
        .copy(metadataCleanupInterval = Some(20.millis), period = Fixed(15.millis))
        .periodMillis(currentTime, 0) shouldEqual (currentTime - 15)
    }
    "period set to startOfPreviousBucket for max period if metadataCleanupInterval not set" in {
      baseConfig.copy(metadataCleanupInterval = None, period = Max).periodMillis(currentTime, 49) shouldEqual 49
    }
    "cap period at the more recent of (currentTime - metadataCleanupInterval) and start of previous bucket" in {
      baseConfig
        .copy(metadataCleanupInterval = Some(100.millis), period = Max)
        .periodMillis(currentTime, 99) shouldEqual 99
    }
    "cap period at the later of fixed period and start of previous bucket" in {
      baseConfig.copy(period = Fixed(10.millis)).periodMillis(currentTime, 20) shouldEqual (currentTime - 10)
      baseConfig.copy(period = Fixed(99.millis)).periodMillis(currentTime, 90) shouldEqual 90
    }

    "long period not be earlier than metadataCleanupInterval for max long period" in {
      baseConfig
        .copy(metadataCleanupInterval = Some(20.millis), period = Fixed(1.millis), longPeriod = Max)
        .longPeriodMillis(currentTime, 0) shouldEqual (currentTime - 20)
    }
    "use (current time -fixed long period) if set" in {
      baseConfig
        .copy(metadataCleanupInterval = Some(20.millis), longPeriod = Fixed(15.millis))
        .longPeriodMillis(currentTime, 0) shouldEqual (currentTime - 15)
    }
    "go no later than startOfPreviousBucket for max period if metadataCleanupInterval not set" in {
      val startOfPreviousBucket = 50
      baseConfig
        .copy(metadataCleanupInterval = None, longPeriod = Max)
        .longPeriodMillis(currentTime, startOfPreviousBucket) shouldEqual startOfPreviousBucket
    }
    "set long period to the later of (current time - metadataCleanupInterval) and start of previous bucket" in {
      val startOfPreviousBucket = 99
      baseConfig
        .copy(metadataCleanupInterval = Some(100.millis), longPeriod = Max)
        .longPeriodMillis(currentTime, startOfPreviousBucket) shouldEqual startOfPreviousBucket
    }
    "cap long-period at the later of fixed period and start of previous bucket" in {
      baseConfig.copy(longPeriod = Fixed(10.millis)).longPeriodMillis(currentTime, 80) shouldEqual (currentTime - 10)
      baseConfig.copy(longPeriod = Fixed(99.millis)).longPeriodMillis(currentTime, 80) shouldEqual 80
    }

    "disallow only setting a long interval" in {
      intercept[IllegalArgumentException] {
        BackTrackConfig(None, None, Max, Some(10.seconds), Max)
      }.getMessage should include("interval must be enabled to use long-interval")
    }
    "disallow long interval being shorter than interval" in {
      intercept[IllegalArgumentException] {
        BackTrackConfig(None, Some(11.seconds), Max, Some(10.seconds), Max)
      }.getMessage should include("interval must be smaller than long-interval")
    }
    "disallow periods being within 10% of metadataCleanupInterval" in {
      intercept[IllegalArgumentException] {
        BackTrackConfig(Some(10.seconds), None, Fixed(9500.millis), None, Max)
      }.getMessage should include("period has to be at least 10% lower than cleanup-old-persistence-ids")

      intercept[IllegalArgumentException] {
        BackTrackConfig(Some(10.seconds), None, Max, None, Fixed(9500.millis))
      }.getMessage should include("long-period has to be at least 10% lower than cleanup-old-persistence-ids")
    }
  }
}
