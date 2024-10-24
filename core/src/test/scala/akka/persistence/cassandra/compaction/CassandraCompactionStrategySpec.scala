/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import java.util.concurrent.TimeUnit

import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, PluginSettings }
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.persistence.cassandra.RequiresCassandraThree

object CassandraCompactionStrategySpec {
  lazy val config = ConfigFactory.parseString(s"""
       |akka.persistence.cassandra.journal.keyspace=CassandraCompactionStrategySpec
       |akka.persistence.cassandra.snapshot.keyspace=CassandraCompactionStrategySpecSnapshot
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class CassandraCompactionStrategySpec
    extends CassandraSpec(CassandraCompactionStrategySpec.config)
    with AnyWordSpecLike {

  val cassandraPluginSettings = PluginSettings(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cluster.execute(
      "CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  }

  "A CassandraCompactionStrategy" must {

    "successfully create a TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 10
          | compaction_window_unit = "DAYS"
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(twConfig.getConfig("journal.table-compaction-strategy"))
        .asInstanceOf[TimeWindowCompactionStrategy]

      compactionStrategy.compactionWindowSize shouldEqual 10
      compactionStrategy.compactionWindowUnit shouldEqual TimeUnit.DAYS
    }

    "successfully create CQL from TimeWindowCompactionStrategy" taggedAs (RequiresCassandraThree) in {
      val twConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 1
          | compaction_window_unit = "MINUTES"
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(
          twConfig.getConfig("journal.table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        cluster.execute(cqlExpression)
      }
    }

    "fail if unrecognised property for TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 10
          | banana = "cherry"
          |}
        """.stripMargin)

      assertThrows[IllegalArgumentException] {
        CassandraCompactionStrategy(twConfig.getConfig("journal.table-compaction-strategy"))
          .asInstanceOf[TimeWindowCompactionStrategy]
      }
    }

    "successfully create a LeveledCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
          | class = "LeveledCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | sstable_size_in_mb = 100
          |}
        """.stripMargin)

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("journal.table-compaction-strategy"))
        .asInstanceOf[LeveledCompactionStrategy]

      compactionStrategy.enabled shouldEqual true
      compactionStrategy.tombstoneCompactionInterval shouldEqual 86400
      compactionStrategy.tombstoneThreshold shouldEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction shouldEqual false
      compactionStrategy.ssTableSizeInMB shouldEqual 100
    }

    "successfully create CQL from LeveledCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
          | class = "LeveledCompactionStrategy"
          | enabled = true
          | tombstone_compaction_interval = 86400
          | tombstone_threshold = 0.1
          | unchecked_tombstone_compaction = false
          | sstable_size_in_mb = 100
          |}
        """.stripMargin)

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable2 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(
          uniqueConfig.getConfig("journal.table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        cluster.execute(cqlExpression)
      }
    }

    "successfully create a SizeTieredCompactionStrategy" in {
      val uniqueConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
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

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("journal.table-compaction-strategy"))
        .asInstanceOf[SizeTieredCompactionStrategy]

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
      val uniqueConfig = ConfigFactory.parseString("""journal.table-compaction-strategy {
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
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable3 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(
          uniqueConfig.getConfig("journal.table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        cluster.execute(cqlExpression)
      }
    }
  }
}
