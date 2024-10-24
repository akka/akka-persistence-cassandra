/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import com.typesafe.config.Config

import scala.concurrent.duration._

@InternalApi
private[akka] final class HealthCheckSettings(system: ActorSystem, config: Config)
    extends NoSerializationVerificationNeeded {

  private val healthCheckConfig = config.getConfig("healthcheck")

  val pluginLocation: String = healthCheckConfig.getString("plugin-location")

  val timeout: FiniteDuration = healthCheckConfig.getDuration("timeout", MILLISECONDS).millis

  val healthCheckCql: String = healthCheckConfig.getString("health-check-cql")

}
