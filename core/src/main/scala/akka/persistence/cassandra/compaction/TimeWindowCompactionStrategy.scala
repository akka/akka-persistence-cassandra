/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.collection.JavaConverters._

class TimeWindowCompactionStrategy(config: Config) extends BaseCompactionStrategy(config) {
  import TimeWindowCompactionStrategy._

  require(config.hasPath("class") && config.getString("class") == ClassName, s"Config does not specify a $ClassName")

  require(
    config.entrySet()
      .asScala
      .map(_.getKey)
      .forall(propertyKeys.contains(_)),
    s"Config contains properties not supported by a $ClassName. Supported: $propertyKeys. Supplied: ${config.entrySet().asScala.map(_.getKey)}"
  )

  val compactionWindowUnit: TimeUnit = if (config.hasPath("compaction_window_unit")) TimeUnit.valueOf(config.getString("compaction_window_unit")) else TimeUnit.DAYS
  val compactionWindowSize: Int = if (config.hasPath("compaction_window_size")) config.getInt("compaction_window_size") else 1

  require(compactionWindowSize >= 1, s"compaction_window_size must be larger than or equal to 1, but was $compactionWindowSize")

  override def asCQL: String =
    s"""{
       |'class' : '$ClassName',
       |${super.asCQL},
       |'compaction_window_size' : $compactionWindowSize,
       |'compaction_window_unit' : '${compactionWindowUnit.toString.toUpperCase}'
       |}
     """.stripMargin.trim
}

object TimeWindowCompactionStrategy extends CassandraCompactionStrategyConfig[TimeWindowCompactionStrategy] {
  override val ClassName: String = "TimeWindowCompactionStrategy"

  override def propertyKeys: List[String] = (
    BaseCompactionStrategy.propertyKeys union List(
      "compaction_window_size",
      "compaction_window_unit"
    )
  ).sorted

  override def fromConfig(config: Config): TimeWindowCompactionStrategy = new TimeWindowCompactionStrategy(config)
}
