/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config
import akka.persistence.query.javadsl.ReadJournal
import scala.util.control.NonFatal

class CassandraReadJournalProvider(
  system:     ExtendedActorSystem,
  config:     Config,
  configPath: String
) extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.CassandraReadJournal =
    try {
      new scaladsl.CassandraReadJournal(system, config, configPath)
    } catch {
      case NonFatal(e) =>
        // TODO can be removed when https://github.com/akka/akka/issues/18976 is fixed
        system.log.error(e, "Failed to initialize CassandraReadJournal")
        throw e
    }

  override val javadslReadJournal: javadsl.CassandraReadJournal =
    new javadsl.CassandraReadJournal(scaladslReadJournal)

}
