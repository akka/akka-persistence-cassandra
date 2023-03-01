/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

class CassandraReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {

  private val scaladslReadJournalInstance = new scaladsl.CassandraReadJournal(system, config, configPath)

  override def scaladslReadJournal(): scaladsl.CassandraReadJournal = scaladslReadJournalInstance

  private val javadslReadJournalInstance = new javadsl.CassandraReadJournal(scaladslReadJournalInstance)

  override def javadslReadJournal(): javadsl.CassandraReadJournal = javadslReadJournalInstance

}
