/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import scala.concurrent.Future
import com.datastax.driver.core.Session
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext

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
