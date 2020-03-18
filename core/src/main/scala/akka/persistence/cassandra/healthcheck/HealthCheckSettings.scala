/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import java.time.Duration

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import com.typesafe.config.Config

@InternalApi
private[akka] class HealthCheckSettings(system: ActorSystem, config: Config) extends NoSerializationVerificationNeeded {

  private val healthCheckConfig = config.getConfig("akka.persistence.cassandra.healthcheck")

  val pluginLocation: String = healthCheckConfig.getString("plugin-location")

  val timeout: Duration = healthCheckConfig.getDuration("timeout")

}
