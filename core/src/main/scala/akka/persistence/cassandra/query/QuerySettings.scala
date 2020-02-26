/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi
import akka.persistence.cassandra.EventsByTagSettings
import akka.persistence.cassandra.PluginSettings
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi private[akka] class QuerySettings(
    system: ActorSystem,
    config: Config,
    val eventsByTagSettings: EventsByTagSettings)
    extends NoSerializationVerificationNeeded {

  private val queryConfig = config.getConfig("query")

  val readProfile: String = queryConfig.getString("read-profile")
  PluginSettings.checkProfile(system, readProfile)

  val refreshInterval: FiniteDuration =
    queryConfig.getDuration("refresh-interval", MILLISECONDS).millis

  val gapFreeSequenceNumbers: Boolean = queryConfig.getBoolean("gap-free-sequence-numbers")

  val maxBufferSize: Int = queryConfig.getInt("max-buffer-size")

  val deserializationParallelism: Int = queryConfig.getInt("deserialization-parallelism")

  val pluginDispatcher: String = queryConfig.getString("plugin-dispatcher")

  val eventsByPersistenceIdEventTimeout: FiniteDuration =
    queryConfig.getDuration("events-by-persistence-id-gap-timeout", MILLISECONDS).millis

}
