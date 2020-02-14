/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.discovery.Discovery
import akka.dispatch.ExecutionContexts
import akka.util.unused
import akka.util.JavaDurationConverters._
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{ DefaultDriverOption, DriverConfigLoader }

import scala.concurrent.duration.FiniteDuration

/**
 * The implementation of the `SessionProvider` is used for creating the
 * Cassandra Session. By default the [[DefaultSessionProvider]] is building
 * the Cluster from configuration properties but it is possible to
 * replace the implementation of the SessionProvider to reuse another
 * session or override the Cluster builder with other settings.
 *
 * The implementation is defined in configuration `session-provider` property.
 * It may optionally have a constructor with an ActorSystem and Config parameter.
 * The config parameter is the config section of the plugin.
 */
trait CqlSessionProvider {
  def connect()(implicit ec: ExecutionContext): Future[CqlSession]
}

/**
 * Builds a `CqlSession` from the given `config` via [[DriverConfigLoaderFromConfig]].
 *
 * The configuration for the driver is typically the `datastax-java-driver` section of the ActorSystem's
 * configuration, but it's possible to use other configuration. The configuration path of the
 * driver's configuration can be defined with `datastax-java-driver-config` property in the
 * given `config`.
 */
class DefaultSessionProvider(system: ActorSystem, config: Config) extends CqlSessionProvider {

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    val builder = CqlSession.builder()
    val driverConfig = CqlSessionProvider.driverConfig(system, config)
    val driverConfigLoader = DriverConfigLoaderFromConfig.fromConfig(driverConfig)
    builder.withConfigLoader(driverConfigLoader).buildAsync().toScala
  }
}

/**
 * ```
 * akka {
 *   discovery.method = config
 * }
 * akka.discovery.config.services = {
 *   cassandra-service = {
 *     endpoints = [
 *       {
 *         host = "127.0.0.1"
 *         port = 9042
 *       },
 *       {
 *         host = "127.0.0.2"
 *         port = 9042
 *       }
 *     ]
 *   }
 * }
 * my-cassandra-session {
 *   session-provider = "akka.cassandra.session.AkkaDiscoverySessionProvider"
 *   session-name = "my-session"
 *   service {
 *     name = "cassandra-service"
 *     lookup-timeout = 1 s
 *   }
 * }
 * ```
 */
class AkkaDiscoverySessionProvider(system: ActorSystem, config: Config) extends CqlSessionProvider {

  /**
   * Use Akka Discovery to read the addresses for `serviceName` within `lookupTimeout`.
   */
  private def readNodes(serviceName: String, lookupTimeout: FiniteDuration)(
      implicit system: ActorSystem): Future[immutable.Seq[String]] = {
    Discovery(system).discovery
      .lookup(serviceName, lookupTimeout)
      .map { resolved =>
        resolved.addresses.map(target => target.host + ":" + target.port)
      }(ExecutionContexts.sameThreadExecutionContext)
  }

  /**
   * Expect a `service` section in Config and use Akka Discovery to read the addresses for `name` within `lookup-timeout`.
   */
  private def readNodes(config: Config)(implicit system: ActorSystem): Future[immutable.Seq[String]] = {
    val serviceConfig = config.getConfig("service")
    val serviceName = serviceConfig.getString("name")
    val lookupTimeout = serviceConfig.getDuration("lookup-timeout").asScala
    readNodes(serviceName, lookupTimeout)
  }

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    val driverConfig = CqlSessionProvider.driverConfig(system, config)
    val sessionName =
      if (driverConfig.hasPath("basic.session-name")) driverConfig.getString("basic.session-name")
      else ""
    require(sessionName != null && sessionName.nonEmpty, s"driver config needs to set a non-empty `basic.session-name`")

    readNodes(config)(system).flatMap { contactPoints =>
      val driverConfigWithContactPoints = ConfigFactory.parseString(s"""
        basic.contact-points = [${contactPoints.mkString("\"", "\", \"", "\"")}]
        """).withFallback(driverConfig)

      val driverConfigLoader = DriverConfigLoaderFromConfig.fromConfig(driverConfigWithContactPoints)
      CqlSession.builder().withConfigLoader(driverConfigLoader).buildAsync().toScala
    }
  }

}

object CqlSessionProvider {

  /**
   * Create a `SessionProvider` from configuration.
   * The `session-provider` config property defines the fully qualified
   * class name of the SessionProvider implementation class. It may optionally
   * have a constructor with an `ActorSystem` and `Config` parameter.
   */
  def apply(system: ExtendedActorSystem, config: Config): CqlSessionProvider = {
    val className = config.getString("session-provider")
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val clazz = dynamicAccess.getClassFor[CqlSessionProvider](className).get
    def instantiate(args: immutable.Seq[(Class[_], AnyRef)]) =
      dynamicAccess.createInstanceFor[CqlSessionProvider](clazz, args)

    val params = List((classOf[ActorSystem], system), (classOf[Config], config))
    instantiate(params)
      .recoverWith {
        case x: NoSuchMethodException => instantiate(params.take(1))
      }
      .recoverWith { case x: NoSuchMethodException => instantiate(Nil) }
      .recoverWith {
        case ex: Exception =>
          Failure(
            new IllegalArgumentException(
              s"Unable to create SessionProvider instance for class [$className], " +
              "tried constructor with ActorSystem, Config, and only ActorSystem, and no parameters",
              ex))
      }
      .get
  }

  /**
   * The `Config` for the `datastax-java-driver`. The configuration path of the
   * driver's configuration can be defined with `datastax-java-driver-config` property in the
   * given `config`. `datastax-java-driver` configuration section is also used as fallback.
   */
  def driverConfig(system: ActorSystem, config: Config): Config = {
    val driverConfigPath = config.getString("datastax-java-driver-config")
    system.settings.config.getConfig(driverConfigPath).withFallback {
      if (driverConfigPath == "datastax-java-driver") ConfigFactory.empty()
      else system.settings.config.getConfig("datastax-java-driver")
    }
  }
}
