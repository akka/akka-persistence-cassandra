/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

class CassandraReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.CassandraReadJournal =
    new scaladsl.CassandraReadJournal(system, config, configPath)

  override val javadslReadJournal: javadsl.CassandraReadJournal =
    new javadsl.CassandraReadJournal(scaladslReadJournal)

}
