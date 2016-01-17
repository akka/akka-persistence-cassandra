/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import akka.persistence.cassandra.testkit.CassandraLauncher
import java.util.concurrent.TimeUnit

import akka.persistence.cassandra.{ CassandraPluginConfig, CassandraLifecycle }
import com.datastax.driver.core.{ Session, Cluster }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ MustMatchers, WordSpec }

class CassandraCompactionStrategySpec extends WordSpec with MustMatchers with CassandraLifecycle {
  val defaultConfigs = ConfigFactory.parseString(
    s"""keyspace-autocreate = true
      |keyspace = test-keyspace
      |connect-retries = 3
      |connect-retry-delay = 5s
      |connection-pool {
      |  new-connection-threshold-local = 100
      |  new-connection-threshold-remote = 100
      |  connections-per-host-core-local = 1
      |  connections-per-host-max-local = 1
      |  connections-per-host-core-remote = 1
      |  connections-per-host-max-remote = 1
      |  max-requests-per-connection-local = 32768
      |  max-requests-per-connection-remote = 2000
      |  pool-timeout-millis = 0
      |}
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
      |port = ${CassandraLauncher.randomPort}
      |max-result-size = 50
      |delete-retries = 4
    """.stripMargin
  )

  val cassandraPluginConfig = new CassandraPluginConfig(defaultConfigs)

  var cluster: Cluster = _
  var session: Session = _

  import cassandraPluginConfig._

  override def systemName: String = "CassandraCompactionStrategySpec"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    cluster = clusterBuilder.build()
    session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  }

  override protected def afterAll(): Unit = {
    session.close()
    cluster.close()

    super.afterAll()
  }

  "A CassandraCompactionStrategy" must {
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
