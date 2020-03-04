/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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

/**
 * Tool for deleting all events and/or snapshots for a given list of `persistenceIds` without using persistent actors.
 * It's important that the actors with corresponding `persistenceId` are not running
 * at the same time as using the tool.
 *
 * If `neverUsePersistenceIdAgain` is `true` then the highest used sequence number is deleted and
 * the `persistenceId` should not be used again, since it would be confusing to reuse the same sequence
 * numbers for new events.
 *
 * When a list of `persistenceIds` are given they are deleted sequentially in the order
 * of the list. It's possible to parallelize the deletes by running several cleanup operations
 * at the same time operating on different sets of `persistenceIds`.
 */
@ApiMayChange
final class Cleanup(system: ActorSystem, settings: CleanupSettings) {

  def this(system: ActorSystem) =
    this(system, new CleanupSettings(system.settings.config.getConfig("akka.persistence.cassandra.cleanup")))

  import settings._
  import system.dispatcher

  private val log = Logging(system, getClass)
  private val journal = Persistence(system).journalFor(pluginLocation + ".journal")
  private val snapshotStore = Persistence(system).snapshotStoreFor(pluginLocation + ".snapshot")
  private implicit val askTimeout: Timeout = operationTimeout

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAll", pid => deleteAll(pid, neverUsePersistenceIdAgain))
  }

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    deleteAllEvents(persistenceId, neverUsePersistenceIdAgain).flatMap(_ => deleteAllSnapshots(persistenceId))
  }

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAllEvents", pid => deleteAllEvents(pid, neverUsePersistenceIdAgain))
  }

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    (journal ? CassandraJournal.DeleteAllEvents(persistenceId, neverUsePersistenceIdAgain)).mapTo[Done]
  }

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   */
  def deleteAllSnapshots(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "deleteAllSnapshots", pid => deleteAllSnapshots(pid))
  }

  /**
   * Delete all snapshots related to one single `persistenceId`. Events are not deleted.
   */
  def deleteAllSnapshots(persistenceId: String): Future[Done] = {
    (snapshotStore ? CassandraSnapshotStore.DeleteAllsnapshots(persistenceId)).mapTo[Done]
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
