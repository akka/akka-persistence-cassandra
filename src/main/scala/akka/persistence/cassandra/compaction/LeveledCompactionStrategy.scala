/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/*
 * https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/LeveledCompactionStrategy.java
 */
class LeveledCompactionStrategy(config: Config) extends BaseCompactionStrategy(config) {
  require(config.hasPath("class") && config.getString("class") == LeveledCompactionStrategy.ClassName, s"Config does not specify a ${LeveledCompactionStrategy.ClassName}")

  require(
    config.entrySet()
      .asScala
      .map(_.getKey)
      .forall(LeveledCompactionStrategy.propertyKeys.contains(_)),
    s"Config contains properties not supported by a ${LeveledCompactionStrategy.ClassName}"
  )

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
      "sstable_size_in_mb"
    )
  ).sorted

  override def fromConfig(config: Config): LeveledCompactionStrategy = new LeveledCompactionStrategy(config)
}
