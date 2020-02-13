/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import java.lang.{ Long => JLong }
import akka.actor.ActorSystem
import akka.persistence.cassandra.PluginSettings
import akka.Done
import akka.persistence.cassandra.CassandraStatements
import akka.cassandra.session.scaladsl.CassandraSession
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.actor.ExtendedActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.NotUsed
import akka.persistence.cassandra.journal.TimeBucket
import scala.concurrent.ExecutionContext
import java.util.UUID
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.journal.TaggedPreparedStatements
import akka.persistence.cassandra.journal.TagWriters
import akka.persistence.cassandra.journal.TagWriters._

/**
 * Database actions for reconciliation
 *
 * INTERNAL API
 */
@InternalApi
final class ReconciliationSession(session: CassandraSession, statements: CassandraStatements)(
    implicit ec: ExecutionContext) {
  private val deleteTagView = session.prepare(statements.journalStatements.deleteTag)
  private val deleteTagProgress = session.prepare(statements.journalStatements.deleteTagProgress)
  private val deleteTagScanning = session.prepare(statements.journalStatements.deleteTagScanning)
  private val selectAllTagProgressPs = session.prepare(statements.journalStatements.selectAllTagProgress)

  def deleteFromTagView(
      tag: String,
      bucket: TimeBucket,
      timestamp: UUID,
      persistenceId: String,
      tagPidSequenceNr: Long): Future[Done] = {
    deleteTagView.flatMap(ps =>
      session.executeWrite(ps.bind(tag, bucket.key: JLong, timestamp, persistenceId, tagPidSequenceNr: JLong)))
  }

  /**
   * Delete entries in the tag_writes_progress table for the tag and persistence id
   */
  def deleteTagProgress(tag: String, persistenceId: String): Future[Done] = {
    deleteTagProgress.flatMap(ps => session.executeWrite(ps.bind(persistenceId, tag)))
  }

  /**
   * Delete the tag scanning for the persistence id. Will slow down recovery for the
   * persistence id next time it starts if it has a snapshot as it will need to scan
   * the pre-snapshot events for tags.
   */
  def deleteTagScannning(persistenceId: String): Future[Done] = {
    deleteTagScanning.flatMap(ps => session.executeWrite(ps.bind(persistenceId)))
  }

  /**
   * Return the entire tag progress table. A very inefficient query but can be used to get all the
   * current tag names.
   */
  def selectAllTagProgress(): Source[Row, NotUsed] = {
    Source.future(selectAllTagProgressPs.map(ps => session.select(ps.bind()))).flatMapConcat(identity)
  }

  /**
   * Caution: this removes all data from all tag related tables
   */
  def truncateAll(): Future[Done] = {
    val tagViews = session.executeWrite(SimpleStatement.newInstance(statements.journalStatements.truncateTagViews))
    val tagProgress =
      session.executeWrite(SimpleStatement.newInstance(statements.journalStatements.truncateTagProgress))
    val tagScanning =
      session.executeWrite(SimpleStatement.newInstance(statements.journalStatements.truncateTagScanning))

    for {
      _ <- tagViews
      _ <- tagProgress
      _ <- tagScanning
    } yield Done
  }
}

/**
 * For reconciling with tag_views table with the messages table. Can be used to fix data issues causes
 * by split brains or persistence ids running in multiple locations.
 *
 * Should not be run at the same time as an application.
 *
 * To support running in the same system as a journal the tag writers actor would need to be shared
 * and all the interleavings of the actor running at the same be considered.
 *
 * API likely to change when a java/scaladsl is added.
 */
@ApiMayChange
final class Reconciliation(system: ActorSystem) extends Extension {
  import system.dispatcher

  // TODO make session configurable
  private val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra", system.dispatcher)
  private val settings = PluginSettings(system)
  private val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  private val statements = new CassandraStatements(settings)
  private val recSession = new ReconciliationSession(session, statements)
  private val tagStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)

  // Plugin
  // TODO profile from config
  private val tagWriterSession: TagWriters.TagWritersSession =
    new TagWritersSession(
      session,
      "akka-persistence-cassandra-profile",
      "akka-persistence-cassandra-profile",
      tagStatements)
  private val tagWriters =
    system.actorOf(
      TagWriters.props(settings.eventsByTagSettings.tagWriterSettings, tagWriterSession),
      "reconciliation-tag-writers")
  private val recovery = new CassandraTagRecovery(system, session, settings, tagStatements, tagWriters)

  /**
   * Scans the given tag and deletes all events for the provided persistence ids.
   * All events for a persistence id have to be deleted as not to leave gaps in the
   * tag pid sequence numbers.
   */
  def deleteTagViewForPersistenceIds(persistenceId: Set[String], tag: String): Future[Done] =
    new DeleteTagViewForPersistenceId(persistenceId, tag, system, recSession, settings, queries).execute()

  /**
   * Assumes that the tag views table contains no elements for the given persistence ids
   * Either because tag_views and tag_progress have truncated for this given persistence id
   * or tag writing has never been enabled
   *
   * TODO fail fast if there are already events there?
   * TODO support resuming?
   */
  def rebuildTagViewForPersistenceIds(persistenceId: String): Future[Done] =
    new BuildTagViewForPersisetceId(persistenceId, system, recovery, settings).reconcile()

  /**
   * Returns all the tags in the journal. This is not an efficient query for Cassandra so it is better
   * to calculate tags for calls to deleteTagViewForPersistenceId another way.
   */
  def allTags(): Source[String, NotUsed] = new AllTags(recSession).execute()

  /**
   * Truncate all tables and all metadata so that it can be rebuilt
   */
  def truncateTagView(): Future[Done] = recSession.truncateAll()

}

/**
 * An extension for reconciling the tag_views table with the messages table
 */
object Reconciliation extends ExtensionId[Reconciliation] with ExtensionIdProvider {
  def createExtension(system: ExtendedActorSystem): Reconciliation = new Reconciliation(system)
  override def lookup(): Reconciliation.type = Reconciliation
  override def get(system: ActorSystem): Reconciliation = super.get(system)
  override def get(system: ClassicActorSystemProvider): Reconciliation = super.get(system)
}
