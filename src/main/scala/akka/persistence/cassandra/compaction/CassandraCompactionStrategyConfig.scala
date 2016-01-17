/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

trait CassandraCompactionStrategyConfig[CSS <: CassandraCompactionStrategy] {
  val ClassName: String

  def propertyKeys: List[String]

  def fromConfig(config: Config): CSS
}
