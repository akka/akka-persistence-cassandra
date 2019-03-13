/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.compaction

import com.typesafe.config.Config

/*
 * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/compactSubprop.html
 */
trait CassandraCompactionStrategy {
  def asCQL: String
}

object CassandraCompactionStrategy {
  def apply(config: Config): CassandraCompactionStrategy =
    BaseCompactionStrategy.fromConfig(config)
}
