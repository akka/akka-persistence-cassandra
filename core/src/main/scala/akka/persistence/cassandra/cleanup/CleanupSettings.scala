/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import akka.annotation.ApiMayChange
import com.typesafe.config.Config

@ApiMayChange
class CleanupSettings(config: Config) {
  val journalPlugin: String = config.getString("journal-plugin")
  val snapshotPlugin: String = config.getString("snapshot-plugin")
  val operationTimeout: FiniteDuration = config.getDuration("operation-timeout", TimeUnit.MILLISECONDS).millis
  val logProgressEvery: Int = config.getInt("log-progress-every")
  val dryRun: Boolean = config.getBoolean("dry-run")
}
