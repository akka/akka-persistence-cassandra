/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.ActorSystem
import akka.persistence.cassandra.PluginSettings
import akka.Done
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.journal.TaggedPreparedStatements
import akka.persistence.cassandra.CassandraStatements
import akka.cassandra.session.scaladsl.CassandraSession
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import akka.persistence.cassandra.journal.TagWriters._
import akka.persistence.cassandra.journal.TagWriters
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Future

object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Reconciler")
    val settings = PluginSettings(system)
    // FIXME
    val session = CassandraSessionRegistry(system).sessionFor("cassandra-plugin", system.dispatcher)
    val buildTagView = new BuildTagViewForPersisetceId("cat", system, session, settings)
    buildTagView.reconcile()
  }

}

class DeleteTagViewForPersistenceId() {}

class BuildTagViewForPersisetceId(
    persistenceId: String,
    system: ActorSystem,
    session: CassandraSession,
    settings: PluginSettings) {

  import system.dispatcher

  val statements = new CassandraStatements(settings)
  val tagStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)
  val recovery = new CassandraTagRecovery(system, session, settings, tagStatements)
  val tagWriterSession: TagWriters.TagWritersSession =
    new TagWritersSession(session, "cassandra-plugin", "cassandra-plugin", tagStatements)
  val tagWriters = system.actorOf(TagWriters.props(settings.eventsByTagSettings.tagWriterSettings, tagWriterSession))

  // FIXME
  implicit val timeout = Timeout(5.seconds)

  def reconcile(): Future[Done] = {

    for {
      tp <- recovery.lookupTagProgress(persistenceId)
      _ <- recovery.setTagProgress(persistenceId, tp)
    } yield ???

    Future.successful(Done)
  }

}
