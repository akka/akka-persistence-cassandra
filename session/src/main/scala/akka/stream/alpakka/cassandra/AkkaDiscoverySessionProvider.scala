/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.actor.ActorSystem
import akka.discovery.Discovery
import akka.dispatch.ExecutionContexts
import akka.util.JavaDurationConverters._
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

/**
 * A [[CqlSessionProvider]] implementation which builds a `CqlSession` from the given `config`
 * with contact points provided via [[https://doc.akka.io/docs/akka/current/discovery/index.html Akka Discovery]].
 *
 * The configuration for the driver is typically the `datastax-java-driver` section of the ActorSystem's
 * configuration, but it's possible to use other configuration. The configuration path of the
 * driver's configuration can be defined with `datastax-java-driver-config` property in the
 * given `config`.
 *
 * Akka Discovery overwrites the `basic.contact-points` from the configuration with addresses
 * provided by the configured Akka Discovery mechanism.
 *
 * Example using config-based Akka Discovery:
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
 *   service {
 *     name = "cassandra-service"
 *     lookup-timeout = 1 s
 *   }
 *   # Full config path to the Datastax Java driver's configuration section.
 *   # When connecting to more than one Cassandra cluster different session configuration can be
 *   # defined with this property.
 *   datastax-java-driver-config = "datastax-java-driver"
 * }
 * ```
 *
 * Look up this  `CassandraSession` with
 * ```
 * CassandraSessionRegistry
 *   .get(system)
 *   .sessionFor(CassandraSessionSettings.create("my-cassandra-session"), system.dispatcher)
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
