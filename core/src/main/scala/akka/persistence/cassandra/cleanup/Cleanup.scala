/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.pattern.ask
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.snapshot.CassandraSnapshotStore
import akka.util.Timeout

@ApiMayChange
final class Cleanup(system: ActorSystem) {

  // FIXME in 1.0 we should support configurable pluginId
  private val journal = Persistence(system).journalFor("")
  // FIXME handle when default snapshot store isn't configured
  private val snapshotStore = Persistence(system).snapshotStoreFor("")
  private implicit val askTimeout: Timeout = 10.seconds
  import system.dispatcher

  def deleteAllEvents(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    Future.sequence(persistenceIds.map(pid => deleteAllEvents(pid, neverUsePersistenceIdAgain))).map(_ => Done)
  }

  def deleteAllEvents(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    (journal ? CassandraJournal.DeleteAllEvents(persistenceId, neverUsePersistenceIdAgain)).mapTo[Done]
  }

  def deleteAllSnapshots(persistenceIds: immutable.Seq[String]): Future[Done] = {
    Future.sequence(persistenceIds.map(pid => deleteAllSnapshots(pid))).map(_ => Done)
  }

  def deleteAllSnapshots(persistenceId: String): Future[Done] = {
    (snapshotStore ? CassandraSnapshotStore.DeleteAllsnapshots(persistenceId)).mapTo[Done]
  }

}
