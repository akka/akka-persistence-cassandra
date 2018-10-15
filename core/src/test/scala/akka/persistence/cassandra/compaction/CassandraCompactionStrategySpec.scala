/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import java.util.concurrent.TimeUnit

import akka.persistence.cassandra.{ CassandraLifecycle, CassandraPluginConfig, CassandraSpec }
import com.datastax.driver.core.Session
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object CassandraCompactionStrategySpec {
  lazy val config = ConfigFactory.parseString(
    s"""
       |cassandra-journal.keyspace=CassandraCompactionStrategySpec
       |cassandra-snapshot-store.keyspace=CassandraCompactionStrategySpecSnapshot
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class CassandraCompactionStrategySpec extends CassandraSpec(CassandraCompactionStrategySpec.config)
  with WordSpecLike {

  import system.dispatcher

  val defaultConfigs = system.settings.config.getConfig("cassandra-journal")

  val cassandraPluginConfig = new CassandraPluginConfig(system, defaultConfigs)

  var session: Session = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = Await.result(cassandraPluginConfig.sessionProvider.connect(), 5.seconds)
    session.execute("CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  }

  override protected def afterAll(): Unit = {
    Try {
      session.close()
      session.getCluster.close()
    }
    super.afterAll()
  }

  "A CassandraCompactionStrategy" must {

    "successfully create a TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 10
          | compaction_window_unit = "DAYS"
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(twConfig.getConfig("table-compaction-strategy")).asInstanceOf[TimeWindowCompactionStrategy]

      compactionStrategy.compactionWindowSize shouldEqual 10
      compactionStrategy.compactionWindowUnit shouldEqual TimeUnit.DAYS
    }

    "successfully create CQL from TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 1
          | compaction_window_unit = "MINUTES"
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(twConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
      }
    }

    "fail if unrecognised property for TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 10
          | banana = "cherry"
          |}
        """.stripMargin)

      assertThrows[IllegalArgumentException] {
        CassandraCompactionStrategy(twConfig.getConfig("table-compaction-strategy")).asInstanceOf[TimeWindowCompactionStrategy]
      }
    }

    "successfully create a DateTieredCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "DateTieredCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | base_time_seconds = 100
          | max_sstable_age_days = 100
          | max_threshold = 20
          | min_threshold = 10
          | timestamp_resolution = "MICROSECONDS"
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[DateTieredCompactionStrategy]

      compactionStrategy.enabled shouldEqual true
      compactionStrategy.tombstoneCompactionInterval shouldEqual 86400
      compactionStrategy.tombstoneThreshold shouldEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction shouldEqual false
      compactionStrategy.baseTimeSeconds shouldEqual 100
      compactionStrategy.maxSSTableAgeDays shouldEqual 100
      compactionStrategy.maxThreshold shouldEqual 20
      compactionStrategy.minThreshold shouldEqual 10
      compactionStrategy.timestampResolution shouldEqual TimeUnit.MICROSECONDS
    }

    "successfully create CQL from DateTieredCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "DateTieredCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | base_time_seconds = 100
          | max_sstable_age_days = 100
          | max_threshold = 20
          | min_threshold = 10
          | timestamp_resolution = "MICROSECONDS"
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
      }
    }

    "successfully create a LeveledCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "LeveledCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | sstable_size_in_mb = 100
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[LeveledCompactionStrategy]

      compactionStrategy.enabled shouldEqual true
      compactionStrategy.tombstoneCompactionInterval shouldEqual 86400
      compactionStrategy.tombstoneThreshold shouldEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction shouldEqual false
      compactionStrategy.ssTableSizeInMB shouldEqual 100
    }

    "successfully create CQL from LeveledCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "LeveledCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | sstable_size_in_mb = 100
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable2 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
      }
    }

    "successfully create a SizeTieredCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "SizeTieredCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | bucket_high = 5.0
          | bucket_low = 2.5
          | max_threshold = 20
          | min_threshold = 10
          | min_sstable_size = 100
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[SizeTieredCompactionStrategy]

      compactionStrategy.enabled shouldEqual true
      compactionStrategy.tombstoneCompactionInterval shouldEqual 86400
      compactionStrategy.tombstoneThreshold shouldEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction shouldEqual false
      compactionStrategy.bucketHigh shouldEqual 5.0
      compactionStrategy.bucketLow shouldEqual 2.5
      compactionStrategy.maxThreshold shouldEqual 20
      compactionStrategy.minThreshold shouldEqual 10
      compactionStrategy.minSSTableSize shouldEqual 100
    }

    "successfully create CQL from SizeTieredCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "SizeTieredCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | bucket_high = 5.0
          | bucket_low = 2.5
          | max_threshold = 20
          | min_threshold = 10
          | min_sstable_size = 100
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable3 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
      }
    }
  }
}
