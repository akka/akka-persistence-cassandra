/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.persistence.cassandra.{ CassandraPluginConfig, CassandraLifecycle }
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.TestKit
import com.datastax.driver.core.{ Session, Cluster }
import com.typesafe.config.ConfigFactory
import org.scalatest.MustMatchers
import org.scalatest.WordSpecLike
import scala.concurrent.Await

object CassandraCompactionStrategySpec {
  lazy val config = ConfigFactory.parseString(
    s"""
       |cassandra-journal.keyspace=CassandraCompactionStrategySpec
       |cassandra-snapshot-store.keyspace=CassandraCompactionStrategySpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)
}

class CassandraCompactionStrategySpec extends TestKit(
  ActorSystem("CassandraCompactionStrategySpec", CassandraCompactionStrategySpec.config)
)
  with WordSpecLike with MustMatchers with CassandraLifecycle {

  import system.dispatcher

  val defaultConfigs = system.settings.config.getConfig("cassandra-journal")

  val cassandraPluginConfig = new CassandraPluginConfig(system, defaultConfigs)

  var session: Session = _

  import cassandraPluginConfig._

  override def systemName: String = "CassandraCompactionStrategySpec"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    session = Await.result(cassandraPluginConfig.sessionProvider.connect(), 5.seconds)

    session.execute("CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  }

  override protected def afterAll(): Unit = {
    session.close()
    session.getCluster.close()

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
        """.stripMargin
      )

      val compactionStrategy = CassandraCompactionStrategy(twConfig.getConfig("table-compaction-strategy")).asInstanceOf[TimeWindowCompactionStrategy]

      compactionStrategy.compactionWindowSize mustEqual 10
      compactionStrategy.compactionWindowUnit mustEqual TimeUnit.DAYS
    }

    "successfully create CQL from TimeWindowCompactionStrategy" in {
      val twConfig = ConfigFactory.parseString(
        """table-compaction-strategy {
          | class = "TimeWindowCompactionStrategy"
          | compaction_window_size = 1
          | compaction_window_unit = "MINUTES"
          |}
        """.stripMargin
      )

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(twConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
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
        """.stripMargin
      )

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[DateTieredCompactionStrategy]

      compactionStrategy.enabled mustEqual true
      compactionStrategy.tombstoneCompactionInterval mustEqual 86400
      compactionStrategy.tombstoneThreshold mustEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction mustEqual false
      compactionStrategy.baseTimeSeconds mustEqual 100
      compactionStrategy.maxSSTableAgeDays mustEqual 100
      compactionStrategy.maxThreshold mustEqual 20
      compactionStrategy.minThreshold mustEqual 10
      compactionStrategy.timestampResolution mustEqual TimeUnit.MICROSECONDS
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
        """.stripMargin
      )

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
        """.stripMargin
      )

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[LeveledCompactionStrategy]

      compactionStrategy.enabled mustEqual true
      compactionStrategy.tombstoneCompactionInterval mustEqual 86400
      compactionStrategy.tombstoneThreshold mustEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction mustEqual false
      compactionStrategy.ssTableSizeInMB mustEqual 100
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
        """.stripMargin
      )

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
        """.stripMargin
      )

      val compactionStrategy = CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asInstanceOf[SizeTieredCompactionStrategy]

      compactionStrategy.enabled mustEqual true
      compactionStrategy.tombstoneCompactionInterval mustEqual 86400
      compactionStrategy.tombstoneThreshold mustEqual 0.1
      compactionStrategy.uncheckedTombstoneCompaction mustEqual false
      compactionStrategy.bucketHigh mustEqual 5.0
      compactionStrategy.bucketLow mustEqual 2.5
      compactionStrategy.maxThreshold mustEqual 20
      compactionStrategy.minThreshold mustEqual 10
      compactionStrategy.minSSTableSize mustEqual 100
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
        """.stripMargin
      )

      val cqlExpression =
        s"CREATE TABLE IF NOT EXISTS testKeyspace.testTable3 (testId TEXT PRIMARY KEY) WITH compaction = ${CassandraCompactionStrategy(uniqueConfig.getConfig("table-compaction-strategy")).asCQL}"

      noException must be thrownBy {
        session.execute(cqlExpression)
      }
    }
  }
}
