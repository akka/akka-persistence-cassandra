/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import java.util.concurrent.TimeUnit

import akka.persistence.cassandra.compaction.TimeWindowCompactionStrategy._
import com.typesafe.config.Config

class TimeWindowCompactionStrategy(config: Config) extends BaseCompactionStrategy(config, ClassName, propertyKeys) {

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
      "compaction_window_unit")).sorted

  override def fromConfig(config: Config): TimeWindowCompactionStrategy = new TimeWindowCompactionStrategy(config)
}
