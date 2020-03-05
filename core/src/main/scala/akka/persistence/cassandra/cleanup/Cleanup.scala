/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.event.Logging
import akka.pattern.ask
import akka.persistence.Persistence
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.snapshot.CassandraSnapshotStore
import akka.util.Timeout

@ApiMayChange
final class Cleanup(system: ActorSystem, settings: CleanupSettings) {

  def this(system: ActorSystem) =
    this(system, new CleanupSettings(system.settings.config.getConfig("akka.persistence.cassandra.cleanup")))

  import settings._
  import system.dispatcher

  private val log = Logging(system, getClass)
  private val journal = Persistence(system).journalFor(journalPlugin)
  private val snapshotStore =
    if (snapshotPlugin != "" || system.settings.config.getString("akka.persistence.snapshot-store.plugin").nonEmpty)
      Some(Persistence(system).snapshotStoreFor(snapshotPlugin))
    else None
  private implicit val askTimeout: Timeout = operationTimeout

  def deleteAll(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAll", pid => deleteAll(pid, neverUsePersistenceIdAgain))
  }

  def deleteAll(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    deleteAllEvents(persistenceId, neverUsePersistenceIdAgain).flatMap(_ => deleteAllSnapshots(persistenceId))
  }

  def deleteAllEvents(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAllEvents", pid => deleteAllEvents(pid, neverUsePersistenceIdAgain))
  }

  def deleteAllEvents(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    (journal ? CassandraJournal.DeleteAllEvents(persistenceId, neverUsePersistenceIdAgain)).mapTo[Done]
  }

  def deleteAllSnapshots(persistenceIds: immutable.Seq[String]): Future[Done] = {
    if (snapshotStore.isDefined)
      foreach(persistenceIds, "deleteAllSnapshots", pid => deleteAllSnapshots(pid))
    else
      Future.successful(Done)
  }

  def deleteAllSnapshots(persistenceId: String): Future[Done] = {
    snapshotStore match {
      case Some(snapshotStoreRef) =>
        (snapshotStoreRef ? CassandraSnapshotStore.DeleteAllsnapshots(persistenceId)).mapTo[Done]
      case None => Future.successful(Done)
    }
  }

  private def foreach(
      persistenceIds: immutable.Seq[String],
      operationName: String,
      pidOperation: String => Future[Done]): Future[Done] = {
    val size = persistenceIds.size
    log.info("Cleanup started {} of [{}] persistenceId.", operationName, size)

    def loop(remaining: List[String], n: Int): Future[Done] = {
      remaining match {
        case Nil => Future.successful(Done)
        case pid :: tail =>
          pidOperation(pid).flatMap { _ =>
            if (n % logProgressEvery == 0)
              log.info("Cleanup {} [{}] of [{}].", operationName, n, size)
            loop(tail, n + 1)
          }
      }
    }

    val result = loop(persistenceIds.toList, n = 1)

    result.onComplete {
      case Success(_) =>
        log.info("Cleanup completed {} of [{}] persistenceId.", operationName, size)
      case Failure(e) =>
        log.error(e, "Cleanup {} failed.")
    }

    result
  }

}
