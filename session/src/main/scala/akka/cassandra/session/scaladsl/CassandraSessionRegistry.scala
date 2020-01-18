/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cassandra.session.scaladsl

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

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
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CassandraSession manually instead.
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
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CassandraSession manually instead.
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
    new CassandraSession(system, sessionProvider, executionContext, log, metricsCategory = key.configPath, init)
  }

}
