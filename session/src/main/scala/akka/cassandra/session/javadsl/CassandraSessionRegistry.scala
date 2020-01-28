/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ActorSystem
import akka.cassandra.session.{ javadsl, scaladsl, CassandraSessionSettings }
import com.datastax.oss.driver.api.core.CqlSession

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

/**
 * This Cassandra session registry makes it possible to share Cassandra sessions between multiple use sites
 * in the same `ActorSystem` (important for the Cassandra Akka Persistence plugin where it is shared between journal,
 * query plugin and snapshot plugin)
 */
object CassandraSessionRegistry {

  /**
   * Java API: get the session registry
   */
  def get(system: ActorSystem): javadsl.CassandraSessionRegistry =
    new javadsl.CassandraSessionRegistry(scaladsl.CassandraSessionRegistry(system))

}

final class CassandraSessionRegistry private (delegate: scaladsl.CassandraSessionRegistry) {

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(configPath: String, executionContext: ExecutionContext): CassandraSession =
    new javadsl.CassandraSession(delegate.sessionFor(configPath, executionContext))

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * The `init` function will be performed once when the session is created, i.e.
   * if `sessionFor` is called from multiple places with different `init` it will
   * only execute the first.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(
      configPath: String,
      executionContext: ExecutionContext,
      init: java.util.function.Function[CqlSession, CompletionStage[Done]]): CassandraSession =
    new javadsl.CassandraSession(delegate.sessionFor(configPath, executionContext, ses => init(ses).toScala))

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   */
  def sessionFor(settings: CassandraSessionSettings, executionContext: ExecutionContext): CassandraSession =
    new javadsl.CassandraSession(delegate.sessionFor(settings, executionContext))

}
