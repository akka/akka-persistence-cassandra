/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.cassandra.session.SessionProvider
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * Default implementation of the `SessionProvider` that loads the configuration
 * from the cassandra-journal.java-driver-config location
 */
class ConfigSessionProvider(system: ActorSystem, config: Config) extends SessionProvider {

  def connect()(implicit ec: ExecutionContext): Future[CqlSession] = {
    Future.successful(CqlSession.builder().build())
  }
}
