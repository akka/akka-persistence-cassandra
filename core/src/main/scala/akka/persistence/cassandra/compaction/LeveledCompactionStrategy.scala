/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

import LeveledCompactionStrategy._

/*
 * https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/LeveledCompactionStrategy.java
 */
class LeveledCompactionStrategy(config: Config) extends BaseCompactionStrategy(config, ClassName, propertyKeys) {
  val ssTableSizeInMB: Long = if (config.hasPath("sstable_size_in_mb")) config.getLong("sstable_size_in_mb") else 160

  require(ssTableSizeInMB > 0, s"sstable_size_in_mb must be larger than 0, but was $ssTableSizeInMB")

  override def asCQL: String =
    s"""{
       |'class' : '${LeveledCompactionStrategy.ClassName}',
       |${super.asCQL},
       |'sstable_size_in_mb' : $ssTableSizeInMB
       |}
     """.stripMargin.trim
}

object LeveledCompactionStrategy extends CassandraCompactionStrategyConfig[LeveledCompactionStrategy] {
  override val ClassName: String = "LeveledCompactionStrategy"

  override def propertyKeys: List[String] = (
    BaseCompactionStrategy.propertyKeys union List(
      "sstable_size_in_mb")).sorted

  override def fromConfig(config: Config): LeveledCompactionStrategy = new LeveledCompactionStrategy(config)
}
