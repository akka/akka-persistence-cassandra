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
import scala.concurrent.duration._
import scala.language.postfixOps

class AkkaPersistenceCassandraHealthCheck(system: ActorSystem) extends (() => Future[Boolean]) {

  private[akka] val log = Logging.getLogger(system, getClass)
  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val timeout: Timeout = 1 second

  override def apply(): Future[Boolean] = {
    val journalRef = Persistence(system).journalFor("akka.persistence.cassandra.journal")
    (journalRef ? HealthCheckQuery).mapTo[HealthCheckResponse].map(_.result).recoverWith {
      case e: AskTimeoutException =>
        log.debug("Failed to receive health check due to ask timeout: {}", e.getCause(), e)
        Future(false)
      case e: Exception =>
        log.debug("Failed to receive health check due to {}", e.getCause, e)
        Future(false)
    }
  }
}
