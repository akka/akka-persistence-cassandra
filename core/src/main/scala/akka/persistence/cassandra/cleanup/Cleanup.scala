/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import java.lang.{ Integer => JInt, Long => JLong }

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import akka.{ Done, NotUsed }
import akka.actor.{ ActorRef, ClassicActorSystemProvider }
import akka.annotation.ApiMayChange
import akka.event.Logging
import akka.pattern.ask
import akka.persistence.JournalProtocol.DeleteMessagesTo
import akka.persistence.{ Persistence, SnapshotMetadata }
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.reconciler.Reconciliation
import akka.persistence.cassandra.reconciler.ReconciliationSettings
import akka.persistence.cassandra.snapshot.{ CassandraSnapshotStatements, CassandraSnapshotStore }
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.Timeout
import com.datastax.oss.driver.api.core.cql.Row

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
final class Cleanup(systemProvider: ClassicActorSystemProvider, settings: CleanupSettings) {

  def this(systemProvider: ClassicActorSystemProvider) =
    this(
      systemProvider,
      new CleanupSettings(systemProvider.classicSystem.settings.config.getConfig("akka.persistence.cassandra.cleanup")))

  private implicit val system = systemProvider.classicSystem
  import settings._
  import system.dispatcher

  private val log = Logging(system, getClass)

  // operations on journal, snapshotStore and tagViews should be only be done when dry-run = false
  private val journal: ActorRef = Persistence(system).journalFor(pluginLocation + ".journal")
  private lazy val snapshotStore: ActorRef = Persistence(system).snapshotStoreFor(pluginLocation + ".snapshot")
  private lazy val tagViewsReconciliation = new Reconciliation(
    system,
    new ReconciliationSettings(system.settings.config.getConfig(pluginLocation + ".reconciler")))

  private implicit val askTimeout: Timeout = operationTimeout

  private lazy val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(pluginLocation)

  private lazy val pluginSettings = PluginSettings(system, system.settings.config.getConfig(pluginLocation))
  private lazy val statements = new CassandraSnapshotStatements(pluginSettings.snapshotSettings)
  private lazy val selectLatestSnapshotsPs = session.prepare(statements.selectLatestSnapshotMeta)
  private lazy val selectAllSnapshotMetaPs = session.prepare(statements.selectAllSnapshotMeta)

  if (dryRun) {
    log.info("Cleanup running in dry run mode. No operations will be executed against the database, only logged")
  }

  private def issueSnapshotDelete(
      persistenceId: String,
      maxToKeep: Long,
      rows: Seq[Row]): Future[Option[SnapshotMetadata]] = {
    log.debug("issueSnapshotDelete [{}] [{}] [{}]", persistenceId, maxToKeep, rows.size)
    rows match {
      case Nil =>
        log.debug("persistence id [{}] has 0 snapshots, no deletes issued", persistenceId)
        Future.successful(None)
      case fewer if fewer.size < maxToKeep =>
        // no delete required, return the oldest snapshot
        log.debug("Fewer than snapshots than requested for persistence id [{}], no deletes issued", persistenceId)
        Future.successful(
          Some(SnapshotMetadata(persistenceId, fewer.last.getLong("sequence_nr"), fewer.last.getLong("timestamp"))))
      case more =>
        if (log.isDebugEnabled) {
          log.debug(
            "Latest {} snapshots for persistence id [{}] range from {} to {}",
            maxToKeep,
            persistenceId,
            more.head.getLong("sequence_nr"),
            more.last.getLong("sequence_nr"))
        }
        val result =
          SnapshotMetadata(persistenceId, more.last.getLong("sequence_nr"), more.last.getLong("timestamp"))
        if (dryRun) {
          log.info(
            "dry run: CQL: [{}] persistence_id: [{}] sequence_nr [{}]",
            statements.deleteSnapshotsBefore,
            persistenceId,
            result.sequenceNr)
          Future.successful(Some(result))
        } else {
          session
            .executeWrite(statements.deleteSnapshotsBefore, persistenceId, result.sequenceNr: JLong)
            .map(_ => Some(result))
        }

    }
  }

  /**
   * Requires Cassandra 3+
   *
   * Keep all snapshots that occurred after `keepAfter`.
   * If fewer than `snapshotsToKeep` occurred after `keepAfter` at least that many
   * are kept. Setting this to 1 ensures that at least snapshot is kept even if it
   * is older than the `keepAfter`
   *
   * If only N number of snapshot should be kept prefer overload without timestamp
   * as it is more efficient.
   *
   * The returned snapshot metadata can be used to issue deletes for events older than the oldest
   * snapshot.
   *
   * @return the snapshot meta of the oldest remaining snapshot. None if there are no snapshots
   *
   */
  def deleteBeforeSnapshot(
      persistenceId: String,
      snapshotsToKeep: Int,
      keepAfterUnixTimestamp: Long): Future[Option[SnapshotMetadata]] = {
    require(snapshotsToKeep >= 1, "must keep at least one snapshot")
    require(keepAfterUnixTimestamp >= 0, "keepAfter must be greater than 0")
    selectAllSnapshotMetaPs
      .flatMap { ps =>
        val allRows: Source[Row, NotUsed] = session.select(ps.bind(persistenceId))
        allRows.zipWithIndex
          .takeWhile {
            case (row, index) =>
              if (row.getLong("timestamp") > keepAfterUnixTimestamp) {
                true
              } else if (index < snapshotsToKeep) {
                true
              } else {
                false
              }
          }
          .map(_._1)
          .runWith(Sink.seq)
      }
      .flatMap(rows => issueSnapshotDelete(persistenceId, snapshotsToKeep, rows))
  }

  /**
   * Requires Cassandra 3+
   *
   * Keep N snapshots and delete all older snapshots along.
   *
   * This operation is much cheaper than including the timestamp because it can use the primary key and limit.
   *
   * @return the snapshot meta of the oldest remaining snapshot. None if there are no snapshots. This can be used to delete events from before the snapshot.
   */
  def deleteBeforeSnapshot(persistenceId: String, maxSnapshotsToKeep: Int): Future[Option[SnapshotMetadata]] = {
    require(maxSnapshotsToKeep >= 1, "Must keep at least one snapshot")
    val snapshots: Future[immutable.Seq[Row]] = selectLatestSnapshotsPs.flatMap { ps =>
      session.select(ps.bind(persistenceId, maxSnapshotsToKeep: JInt)).runWith(Sink.seq)
    }
    snapshots.flatMap(rows => issueSnapshotDelete(persistenceId, maxSnapshotsToKeep, rows))
  }

  /**
   * Requires Cassandra 3+
   *
   * Delete all events before a sequenceNr for the given persistence id.
   *
   * WARNING: deleting events is generally discouraged in event sourced systems.
   *          once deleted the event by tag view can not be re-built
   *
   * @param persistenceId the persistence id to delete for
   * @param toSequenceNr sequence nr (inclusive) to delete up to
   */
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long): Future[Done] = {
    sendToJournal(replyTo => DeleteMessagesTo(persistenceId, toSequenceNr, replyTo))
  }

  /**
   * Requires Cassandra 3+
   *
   * Deletes all but the last N snapshots and deletes all events before this snapshot
   * Does not delete from the tag_views table
   *
   * WARNING: deleting events is generally discouraged in event sourced systems.
   *          once deleted the event by tag view can not be re-built
   */
  def cleanupBeforeSnapshot(persistenceId: String, nrSnapshotsToKeep: Int): Future[Done] = {
    for {
      oldestSnapshot <- deleteBeforeSnapshot(persistenceId, nrSnapshotsToKeep)
      done <- issueDeleteFromSnapshot(oldestSnapshot)
    } yield done
  }

  /**
   * Requires Cassandra 3+
   *
   * Deletes all events for the given persistence id from before the first after keepAfter.
   * If there are not enough snapshots to satisfy nrSnapshotsToKeep then snapshots before
   * keepAfter will also be kept.
   *
   * WARNING: deleting events is generally discouraged in event sourced systems.
   *          once deleted the event by tag view can not be re-built
   */
  def cleanupBeforeSnapshot(persistenceId: String, nrSnapshotsToKeep: Int, keepAfter: Long): Future[Done] = {
    for {
      oldestSnapshot <- deleteBeforeSnapshot(persistenceId, nrSnapshotsToKeep, keepAfter)
      done <- issueDeleteFromSnapshot(oldestSnapshot)
    } yield done
  }

  /**
   * Requires Cassandra 3+
   *
   * See single persistenceId overload for what is done for each persistence id
   */
  def cleanupBeforeSnapshot(persistenceIds: immutable.Seq[String], nrSnapshotsToKeep: Int): Future[Done] = {
    foreach(persistenceIds, "cleanupBeforeSnapshot", pid => cleanupBeforeSnapshot(pid, nrSnapshotsToKeep))
  }

  /**
   * Requires Cassandra 3+
   *
   * See single persistenceId overload for what is done for each persistence id
   */
  def cleanupBeforeSnapshot(
      persistenceIds: immutable.Seq[String],
      nrSnapshotsToKeep: Int,
      keepAfter: Long): Future[Done] = {
    foreach(persistenceIds, "cleanupBeforeSnapshot", pid => cleanupBeforeSnapshot(pid, nrSnapshotsToKeep, keepAfter))
  }

  private def issueDeleteFromSnapshot(snapshot: Option[SnapshotMetadata]): Future[Done] = {
    snapshot match {
      case Some(snapshotMeta) => deleteEventsTo(snapshotMeta.persistenceId, snapshotMeta.sequenceNr)
      case None               => Future.successful(Done)
    }
  }

  /**
   * Delete everything related to the given list of `persistenceIds`. All events, tagged events, and
   * snapshots are deleted.
   */
  def deleteAll(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAll", pid => deleteAll(pid, neverUsePersistenceIdAgain))
  }

  /**
   * Delete everything related to one single `persistenceId`. All events,  tagged events, and
   * snapshots are deleted.
   */
  def deleteAll(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    for {
      _ <- deleteAllEvents(persistenceId, neverUsePersistenceIdAgain)
      _ <- deleteAllSnapshots(persistenceId)
      _ <- deleteAllTaggedEvents(persistenceId)
    } yield Done
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
    sendToJournal(CassandraJournal.DeleteAllEvents(persistenceId, neverUsePersistenceIdAgain))
  }

  /**
   * Delete all events from `tag_views` table related to the given list of `persistenceIds`.
   * Events in `messages` (journal) table are not deleted and snapshots are not deleted.
   */
  def deleteAllTaggedEvents(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "deleteAllEvents", pid => deleteAllTaggedEvents(pid))
  }

  /**
   * Delete all events from `tag_views` table related to one single `persistenceId`.
   * Events in `messages` (journal) table are not deleted and snapshots are not deleted.
   */
  def deleteAllTaggedEvents(persistenceId: String): Future[Done] = {
    tagViewsReconciliation
      .tagsForPersistenceId(persistenceId)
      .flatMap { tags =>
        Future.sequence(tags.map { tag =>
          if (dryRun) {
            log.info("dry run. Delete [{}] tag view for persistence id: [{}]", tag, persistenceId)
            Future.successful(Done)
          } else {
            tagViewsReconciliation.deleteTagViewForPersistenceIds(Set(persistenceId), tag)
          }
        })
      }
      .map(_ => Done)
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
    sendToSnapshotStore(CassandraSnapshotStore.DeleteAllSnapshots(persistenceId))
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
        log.error(e, "Cleanup {} failed.", operationName)
    }

    result
  }

  private def sendToSnapshotStore(msg: Any): Future[Done] = {
    if (dryRun) {
      log.info("dry run: Operation on snapshot store: {}", msg)
      Future.successful(Done)
    } else {
      (snapshotStore ? msg).map(_ => Done)
    }
  }

  private def sendToJournal(msg: Any): Future[Done] = {
    if (dryRun) {
      log.info("dry run: Operation on journal: {}", msg)
      Future.successful(Done)
    } else {
      (journal ? msg).map(_ => Done)
    }
  }

  private def sendToJournal(create: ActorRef => Any): Future[Done] = {
    if (dryRun) {
      log.info("dry run: Operation on journal: {}", create(ActorRef.noSender))
      Future.successful(Done)
    } else {
      import akka.pattern.extended.ask
      ask(journal, create).map(_ => Done)
    }
  }
}
