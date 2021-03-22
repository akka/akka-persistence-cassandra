/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.session

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

object CassandraSessionSettings {
  def apply(config: Config): CassandraSessionSettings =
    new CassandraSessionSettings(config)
}

class CassandraSessionSettings(val config: Config) {
  val fetchSize = config.getInt("max-result-size")
  val readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val connectionRetries: Int = config.getInt("connect-retries")
  val connectionRetryDelay: FiniteDuration =
    config.getDuration("connect-retry-delay", TimeUnit.MILLISECONDS).millis
  val pluginDispatcher: String = config.getString("plugin-dispatcher")
}
