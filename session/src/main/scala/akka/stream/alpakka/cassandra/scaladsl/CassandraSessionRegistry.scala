/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

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
import akka.event.Logging
import akka.stream.alpakka.cassandra.{ CassandraSessionSettings, CqlSessionProvider, FutureDone }
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

  /** Hash key for `sessions`. */
  private case class SessionKey(configPath: String)

  private def sessionKey(settings: CassandraSessionSettings) = SessionKey(settings.configPath)
}

final class CassandraSessionRegistry(system: ExtendedActorSystem) extends Extension {

  import CassandraSessionRegistry._

  private val sessions = new ConcurrentHashMap[SessionKey, CassandraSession]

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Sessions in the session registry are closed after actor system termination.
   */
  def sessionFor(configPath: String, executionContext: ExecutionContext): CassandraSession =
    sessionFor(CassandraSessionSettings(configPath), executionContext)

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
      init: CqlSession => Future[Done]): CassandraSession =
    sessionFor(CassandraSessionSettings(configPath, init), executionContext)

  /**
   * Get an existing session or start a new one with the given settings,
   * makes it possible to share one session across plugins.
   *
   * Note that the session must not be stopped manually, it is shut down when the actor system is shutdown,
   * if you need a more fine grained life cycle control, create the CassandraSession manually instead.
   */
  def sessionFor(settings: CassandraSessionSettings, executionContext: ExecutionContext): CassandraSession = {
    val key = sessionKey(settings)
    sessions.computeIfAbsent(key, _ => startSession(settings, key, executionContext))
  }

  private def startSession(
      settings: CassandraSessionSettings,
      key: SessionKey,
      executionContext: ExecutionContext): CassandraSession = {
    val sessionProvider = CqlSessionProvider(system, system.settings.config.getConfig(key.configPath))
    val log = Logging(system, classOf[CassandraSession])
    new CassandraSession(
      system,
      sessionProvider,
      executionContext,
      log,
      metricsCategory = settings.metricsCategory,
      init = settings.init.getOrElse(_ => FutureDone),
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
