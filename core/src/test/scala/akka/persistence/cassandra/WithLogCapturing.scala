/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.event.Logging._
import akka.testkit.TestEventListener

/**
 * An adaption of TestEventListener that never prints debug logs itself. Use together with [[akka.persistence.cassandra.CassandraSpec]].
 * It allows to enable noisy DEBUG logging for tests and silence pre/post test DEBUG output completely while still showing
 * test-specific DEBUG output selectively
 */
class DebugLogSilencingTestEventListener extends TestEventListener {
  override def print(event: Any): Unit = event match {
    case _: Debug => // ignore
    case _        => super.print(event)
  }
}

/**
 * An adaption of TestEventListener that does not print out any logs. Use together with [[akka.persistence.cassandra.CassandraSpec]].
 * It allows to enable noisy logging for tests and silence pre/post test log output completely while still showing
 * test-specific log output selectively on failures.
 */
class SilenceAllTestEventListener extends TestEventListener {
  override def print(event: Any): Unit = ()
}
