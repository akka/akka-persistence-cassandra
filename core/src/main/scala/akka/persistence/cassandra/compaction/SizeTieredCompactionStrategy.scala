/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

import SizeTieredCompactionStrategy._

/*
 * Based upon https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java
 */
class SizeTieredCompactionStrategy(config: Config) extends BaseCompactionStrategy(config, ClassName, propertyKeys) {

  val bucketHigh: Double = if (config.hasPath("bucket_high")) config.getDouble("bucket_high") else 1.5
  val bucketLow: Double = if (config.hasPath("bucket_low")) config.getDouble("bucket_low") else 0.5
  val maxThreshold: Int = if (config.hasPath("max_threshold")) config.getInt("max_threshold") else 32
  val minThreshold: Int = if (config.hasPath("min_threshold")) config.getInt("min_threshold") else 4
  val minSSTableSize: Long = if (config.hasPath("min_sstable_size")) config.getLong("min_sstable_size") else 50

  require(bucketHigh > bucketLow, s"bucket_high must be larger than bucket_low, but was $bucketHigh")
  require(maxThreshold > 0, s"max_threshold must be larger than 0, but was $maxThreshold")
  require(minThreshold > 1, s"min_threshold must be larger than 1, but was $minThreshold")
  require(maxThreshold > minThreshold, s"max_threshold must be larger than min_threshold, but was $maxThreshold")
  require(minSSTableSize > 0, s"min_sstable_size must be larger than 0, but was $minSSTableSize")

  override def asCQL: String =
    s"""{
       |'class' : '${SizeTieredCompactionStrategy.ClassName}',
       |${super.asCQL},
       |'bucket_high' : $bucketHigh,
       |'bucket_low' : $bucketLow,
       |'max_threshold' : $maxThreshold,
       |'min_threshold' : $minThreshold,
       |'min_sstable_size' : $minSSTableSize
       |}
     """.stripMargin.trim
}

object SizeTieredCompactionStrategy extends CassandraCompactionStrategyConfig[SizeTieredCompactionStrategy] {
  override val ClassName: String = "SizeTieredCompactionStrategy"

  override def propertyKeys: List[String] = (
    BaseCompactionStrategy.propertyKeys union List(
      "bucket_high",
      "bucket_low",
      "max_threshold",
      "min_threshold",
      "min_sstable_size")).sorted

  override def fromConfig(config: Config): SizeTieredCompactionStrategy = new SizeTieredCompactionStrategy(config)
}
