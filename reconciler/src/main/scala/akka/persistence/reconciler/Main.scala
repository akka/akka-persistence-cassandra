/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.persistence.cassandra.journal.TagWriters
import akka.persistence.cassandra.journal.EventsByTagRecovery
import akka.cassandra.session.scaladsl.CassandraSession
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.TaggedPreparedStatements
import akka.persistence.cassandra.journal.CassandraTagRecovery

/**
 * Utility to reconcile the data created by the Akka Persistence Cassandra plugin.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val system: ActorSystem = ActorSystem("Reconciler")
    implicit val ec = system.dispatcher
    val configPath = "cassandra-plugin"
    val session: CassandraSession = CassandraSessionRegistry(system).sessionFor(configPath, system.dispatcher)
    val config = system.settings.config.getConfig(configPath)
    val settings: PluginSettings = new PluginSettings(system, config)
    val tagSettings = settings.eventsByTagSettings
    val statements = new CassandraStatements(settings)
    val taggedStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)
    val tagWriterSession: TagWritersSession = new TagWritersSession(session, settings.journalSettings.writeProfile, settings.journalSettings.readProfile, taggedStatements)

    val serialization = SerializationExtension(system)

    val tagWriters = system.actorOf(TagWriters.props(tagSettings.tagWriterSettings, tagWriterSession))

    val eventsByTagRecovery = new CassandraTagRecovery(system, session, settings.journalSettings, taggedStatements)

    Thread.sleep(5000)

    system.terminate()
  }
}
