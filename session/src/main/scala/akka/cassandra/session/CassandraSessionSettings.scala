/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session

import java.util.concurrent.TimeUnit

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.Config

object CassandraSessionSettings {
  def apply(config: Config): CassandraSessionSettings =
    new CassandraSessionSettings(config)
}

class CassandraSessionSettings(val config: Config) {
  val fetchSize: Int = config.getInt("max-result-size")
  val readConsistency: ConsistencyLevel =
    DefaultConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel =
    DefaultConsistencyLevel.valueOf(config.getString("write-consistency"))
  val connectionRetries: Int = config.getInt("connect-retries")
  val connectionRetryDelay: FiniteDuration =
    config.getDuration("connect-retry-delay", TimeUnit.MILLISECONDS).millis
}
