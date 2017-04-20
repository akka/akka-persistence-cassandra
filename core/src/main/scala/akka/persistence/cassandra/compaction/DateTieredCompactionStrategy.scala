/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/*
 * Based upon https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/DateTieredCompactionStrategy.java
 */
class DateTieredCompactionStrategy(config: Config) extends BaseCompactionStrategy(config) {
  require(config.hasPath("class") && config.getString("class") == DateTieredCompactionStrategy.ClassName, s"Config does not specify a ${DateTieredCompactionStrategy.ClassName}")

  require(
    config.entrySet()
      .asScala
      .map(_.getKey)
      .forall(DateTieredCompactionStrategy.propertyKeys.contains(_)),
    s"Config contains properties not supported by a ${DateTieredCompactionStrategy.ClassName}"
  )

  val baseTimeSeconds: Long = if (config.hasPath("base_time_seconds")) config.getLong("base_time_seconds") else 3600
  val maxSSTableAgeDays: Int = if (config.hasPath("max_sstable_age_days")) config.getInt("max_sstable_age_days") else 365
  val maxThreshold: Int = if (config.hasPath("max_threshold")) config.getInt("max_threshold") else 32
  val minThreshold: Int = if (config.hasPath("min_threshold")) config.getInt("min_threshold") else 4
  val timestampResolution: TimeUnit = if (config.hasPath("timestamp_resolution")) TimeUnit.valueOf(config.getString("timestamp_resolution")) else TimeUnit.MICROSECONDS

  require(baseTimeSeconds > 0, s"base_time_seconds must be greater than 0, but was $baseTimeSeconds")
  require(maxSSTableAgeDays >= 0, s"max_sstable_age_days must be larger than 0, but was $maxSSTableAgeDays")
  require(maxThreshold > 0, s"max_threshold must be larger than 0, but was $maxThreshold")
  require(minThreshold > 1, s"min_threshold must be larger than 1, but was $minThreshold")
  require(maxThreshold > minThreshold, s"max_threshold must be larger than min_threshold, but was $maxThreshold")
  require(timestampResolution == TimeUnit.MICROSECONDS || timestampResolution == TimeUnit.MILLISECONDS, s"timestamp_resolution $timestampResolution is not valid")

  override def asCQL: String =
    s"""{
       |'class' : '${DateTieredCompactionStrategy.ClassName}',
       |${super.asCQL},
       |'base_time_seconds' : $baseTimeSeconds,
       |'max_sstable_age_days' : $maxSSTableAgeDays,
       |'max_threshold' : $maxThreshold,
       |'min_threshold' : $minThreshold,
       |'timestamp_resolution' : '${timestampResolution.toString.toUpperCase}'
       |}
     """.stripMargin.trim
}

object DateTieredCompactionStrategy extends CassandraCompactionStrategyConfig[DateTieredCompactionStrategy] {
  override val ClassName: String = "DateTieredCompactionStrategy"

  override def propertyKeys: List[String] = (
    BaseCompactionStrategy.propertyKeys union List(
      "base_time_seconds",
      "max_sstable_age_days",
      "max_threshold",
      "min_threshold",
      "timestamp_resolution"
    )
  ).sorted

  override def fromConfig(config: Config): DateTieredCompactionStrategy = new DateTieredCompactionStrategy(config)
}
