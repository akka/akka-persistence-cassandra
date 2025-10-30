/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

trait CassandraCompactionStrategyConfig[CSS <: CassandraCompactionStrategy] {
  val ClassName: String

  def propertyKeys: List[String]

  def fromConfig(config: Config): CSS
}
