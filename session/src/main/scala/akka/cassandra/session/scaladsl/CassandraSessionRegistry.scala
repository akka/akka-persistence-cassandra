/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session.scaladsl

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.collection.JavaConverters._
import akka.Done
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.cassandra.session.CqlSessionProvider
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession

/**
 * This Cassandra session registry makes it possible to share Cassandra sessions between multiple use sites
 * in the same `ActorSystem` (important for the Cassandra Akka Persistence plugin where it is shared between journal,
 * query plugin and snapshot plugin)
 */
object CassandraSessionRegistry extends ExtensionId[CassandraSessionRegistry] with ExtensionIdProvider {

  def createExtension(system: ExtendedActorSystem): CassandraSessionRegistry =
    new CassandraSessionRegistry(system)

  /**
   * Java API: get the session registry
   */
  override def get(system: ActorSystem): CassandraSessionRegistry =
    super.get(system)

  override def lookup(): ExtensionId[CassandraSessionRegistry] = this

  private case class SessionKey(configPath: String)
}

final class CassandraSessionRegistry(system: ExtendedActorSystem) extends Extension {

  import CassandraSessionRegistry.SessionKey

  private val sessions = new ConcurrentHashMap[SessionKey, CassandraSession]

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(configPath: String, executionContext: ExecutionContext): CassandraSession =
    sessionFor(configPath, executionContext, _ => Future.successful(Done))

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
      init: CqlSession => Future[Done]): CassandraSession = {
    val key = SessionKey(configPath)
    sessions.computeIfAbsent(key, _ => startSession(key, init, executionContext))
  }

  private def startSession(
      key: SessionKey,
      init: CqlSession => Future[Done],
      executionContext: ExecutionContext): CassandraSession = {
    val sessionProvider = CqlSessionProvider(system, system.settings.config.getConfig(key.configPath))
    val log = Logging(system, classOf[CassandraSession])
    new CassandraSession(
      system,
      sessionProvider,
      executionContext,
      log,
      metricsCategory = key.configPath,
      init,
      onClose = () => sessions.remove(key))
  }

  /**
   * Closes all registered Cassandra sessions.
   * @param executionContext when used after actor system termination, a different execution context must be provided
   */
  private def close(executionContext: ExecutionContext) = {
    implicit val ec: ExecutionContext = executionContext
    val closing = sessions.values().asScala.map(_.close(ec))
    Future.sequence(closing)
  }

  system.whenTerminated.foreach(_ => close(ExecutionContext.global))(ExecutionContext.global)

}
