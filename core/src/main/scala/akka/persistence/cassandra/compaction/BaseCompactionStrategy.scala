/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.JavaConverters._

/*
 * Based upon https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/AbstractCompactionStrategy.java
 */
abstract class BaseCompactionStrategy(config: Config, className: String, propertyKeys: List[String]) extends CassandraCompactionStrategy {
  require(config.hasPath("class") && config.getString("class") == className, s"Config does not specify a $className")
  require(
    config.entrySet()
      .asScala
      .map(_.getKey)
      .forall(propertyKeys.contains(_)),
    s"Config contains properties not supported by a $className. Supported: $propertyKeys. Supplied: ${config.entrySet().asScala.map(_.getKey)}")

  val enabled: Boolean = if (config.hasPath("enabled")) config.getBoolean("enabled") else true
  val tombstoneCompactionInterval: Long = if (config.hasPath("tombstone_compaction_interval")) config.getLong("tombstone_compaction_interval") else 86400
  val tombstoneThreshold: Double = if (config.hasPath("tombstone_threshold")) config.getDouble("tombstone_threshold") else 0.2
  val uncheckedTombstoneCompaction: Boolean = if (config.hasPath("unchecked_tombstone_compaction")) config.getBoolean("unchecked_tombstone_compaction") else false

  require(tombstoneCompactionInterval > 0, s"tombstone_compaction_interval must be greater than 0, but was $tombstoneCompactionInterval")
  require(tombstoneThreshold > 0, s"tombstone_threshold must be greater than 0, but was $tombstoneThreshold")

  override def asCQL: String =
    s"""'enabled' : $enabled,
       |'tombstone_compaction_interval' : $tombstoneCompactionInterval,
       |'tombstone_threshold' : $tombstoneThreshold,
       |'unchecked_tombstone_compaction' : $uncheckedTombstoneCompaction
     """.stripMargin.trim
}

object BaseCompactionStrategy extends CassandraCompactionStrategyConfig[BaseCompactionStrategy] {
  override val ClassName: String = "BaseCompactionStrategy"

  override def propertyKeys: List[String] = List(
    "class",
    "enabled",
    "tombstone_compaction_interval",
    "tombstone_threshold",
    "unchecked_tombstone_compaction")

  override def fromConfig(config: Config): BaseCompactionStrategy = {
    val className = if (config.hasPath("class")) config.getString("class") else ""

    className match {
      case TimeWindowCompactionStrategy.ClassName =>
        TimeWindowCompactionStrategy.fromConfig(config)
      case DateTieredCompactionStrategy.ClassName =>
        DateTieredCompactionStrategy.fromConfig(config)
      case LeveledCompactionStrategy.ClassName =>
        LeveledCompactionStrategy.fromConfig(config)
      case SizeTieredCompactionStrategy.ClassName =>
        SizeTieredCompactionStrategy.fromConfig(config)
      case _ =>
        SizeTieredCompactionStrategy.fromConfig(
          ConfigFactory.parseString(
            s"""
               |class = "${SizeTieredCompactionStrategy.ClassName}"
             """.stripMargin.trim))
    }
  }
}
