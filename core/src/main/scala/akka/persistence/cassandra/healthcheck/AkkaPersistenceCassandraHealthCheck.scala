/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern.{ ask, AskTimeoutException }
import akka.persistence.Persistence
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraJournal.HealthCheckQuery
import akka.util.Timeout

import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class AkkaPersistenceCassandraHealthCheck(system: ActorSystem) extends (() => Future[Boolean]) {

  private[akka] val log = Logging.getLogger(system, getClass)

  private val settings = new PluginSettings(system, system.settings.config.getConfig("akka.persistence.cassandra"))
  private val healthCheckSettings = settings.healthCheckSettings
  private val journalPluginId = s"${healthCheckSettings.pluginLocation}.journal"
  private val journalRef = Persistence(system).journalFor(journalPluginId)

  private implicit val ec: ExecutionContextExecutor = system.dispatchers.lookup(s"$journalPluginId.plugin-dispatcher")
  private implicit val timeout: Timeout = Duration.fromNanos(healthCheckSettings.timeout.toNanos)

  override def apply(): Future[Boolean] = {
    (journalRef ? HealthCheckQuery).map(_ => true).recoverWith {
      case _: AskTimeoutException =>
        log.warning("Failed to execute health check due to ask timeout")
        Future(false)
      case NonFatal(e) =>
        log.warning("Failed to execute health check due to: {}", e)
        Future(false)
    }
  }
}
