/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.collection.immutable
import scala.concurrent.Future
import com.datastax.driver.core.Session
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext
import akka.actor.ExtendedActorSystem
import akka.actor.ActorSystem
import scala.util.Failure

/**
 * The implementation of the `SessionProvider` is used for creating the
 * Cassandra Session. By default the [[ConfigSessionProvider]] is building
 * the Cluster from configuration properties but it is possible to
 * replace the implementation of the SessionProvider to reuse another
 * session or override the Cluster builder with other settings.
 *
 * The implementation is defined in configuration `session-provider` property.
 * It may optionally have a constructor with an ActorSystem and Config parameter.
 * The config parameter is the config section of the plugin.
 */
trait SessionProvider {

  def connect()(implicit ec: ExecutionContext): Future[Session]

}

object SessionProvider {

  /**
   * Create a `SessionProvider` from configuration.
   * The `session-provider` config property defines the fully qualified
   * class name of the SessionProvider implementation class. It may optionally
   * have a constructor with an `ActorSystem` and `Config` parameter.
   */
  def apply(system: ExtendedActorSystem, config: Config): SessionProvider = {
    val className = config.getString("session-provider")
    val dynamicAccess = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
    val clazz = dynamicAccess.getClassFor[SessionProvider](className).get
    def instantiate(args: immutable.Seq[(Class[_], AnyRef)]) =
      dynamicAccess.createInstanceFor[SessionProvider](clazz, args)

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
}
