/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern.{ ask, AskTimeoutException }
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.CassandraJournal.{ HealthCheckQuery, HealthCheckResponse }
import akka.util.Timeout

import scala.concurrent.{ ExecutionContextExecutor, Future }
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.util.control.NonFatal

class AkkaPersistenceCassandraHealthCheck(system: ActorSystem) extends (() => Future[Boolean]) {

  private[akka] val log = Logging.getLogger(system, getClass)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val healthCheckSettings = new HealthCheckSettings(system, system.settings.config)
  private val journalPluginId = s"${healthCheckSettings.pluginLocation}.journal"
  private val journalRef = Persistence(system).journalFor(journalPluginId)

  private implicit val timeout: Timeout = Timeout(healthCheckSettings.timeoutMs, MILLISECONDS)

  override def apply(): Future[Boolean] = {
    (journalRef ? HealthCheckQuery).mapTo[HealthCheckResponse].map(_.result).recoverWith {
      case e: AskTimeoutException =>
        log.debug("Failed to execute health check due to ask timeout", e)
        Future(false)
      case NonFatal(e) =>
        log.debug("Failed to execute health check", e)
        Future(false)
    }
  }
}
