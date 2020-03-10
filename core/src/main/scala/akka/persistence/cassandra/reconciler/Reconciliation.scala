/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.stream.scaladsl.Sink
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.CassandraStatements
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.journal.TaggedPreparedStatements
import akka.persistence.cassandra.journal.TagWriters
import akka.persistence.cassandra.journal.TagWriters._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import java.util.UUID
import java.lang.{ Long => JLong }
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ExtendedActorSystem

/**
 * Database actions for reconciliation
 *
 * INTERNAL API
 */
@InternalApi
final private[akka] class ReconciliationSession(session: CassandraSession, statements: CassandraStatements)(
    implicit ec: ExecutionContext) {
  private val deleteTagView = session.prepare(statements.journalStatements.deleteTag)
  private val deleteTagProgress = session.prepare(statements.journalStatements.deleteTagProgress)
  private val deleteTagScanning = session.prepare(statements.journalStatements.deleteTagScanning)
  private val selectAllTagProgressPs = session.prepare(statements.journalStatements.selectAllTagProgress)
  private val selectTagProgressForPersistenceId =
    session.prepare(statements.journalStatements.selectTagProgressForPersistenceId)

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
    Source.futureSource(selectAllTagProgressPs.map(ps => session.select(ps.bind()))).mapMaterializedValue(_ => NotUsed)
  }

  def selectTagProgress(persistenceId: String): Source[String, NotUsed] = {
    Source
      .futureSource(
        selectTagProgressForPersistenceId.map(ps => session.select(ps.bind(persistenceId)).map(_.getString("tag"))))
      .mapMaterializedValue(_ => NotUsed)
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
 * INTERNAL API
 */
@InternalApi private[akka] object Reconciliation {
  private val uniqueActorNameCounter = new AtomicInteger(0)
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
final class Reconciliation(system: ActorSystem, settings: ReconciliationSettings) {

  def this(system: ActorSystem) =
    this(system, new ReconciliationSettings(system.settings.config.getConfig("akka.persistence.cassandra.reconciler")))

  import system.dispatcher
  private implicit val sys = system
  private val session = CassandraSessionRegistry(system).sessionFor(settings.pluginLocation)
  private val pluginSettings = PluginSettings(system, system.settings.config.getConfig(settings.pluginLocation))
  private val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](settings.pluginLocation + ".query")
  private val statements = new CassandraStatements(pluginSettings)
  private val recSession = new ReconciliationSession(session, statements)
  private val tagStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)

  private val tagWriterSession: TagWriters.TagWritersSession =
    new TagWritersSession(session, settings.writeProfile, settings.readProfile, tagStatements)
  private val tagWriters =
    system
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(
        TagWriters.props(pluginSettings.eventsByTagSettings.tagWriterSettings, tagWriterSession),
        s"reconciliation-tag-writers-${Reconciliation.uniqueActorNameCounter.incrementAndGet()}")
  private val recovery = new CassandraTagRecovery(system, session, pluginSettings, tagStatements, tagWriters)

  /**
   * Scans the given tag and deletes all events for the provided persistence ids.
   * All events for a persistence id have to be deleted as not to leave gaps in the
   * tag pid sequence numbers.
   *
   * As this has to scan the tag views table for the given tag it is more efficient to
   */
  def deleteTagViewForPersistenceIds(persistenceId: Set[String], tag: String): Future[Done] =
    new DeleteTagViewForPersistenceId(persistenceId, tag, system, recSession, pluginSettings, queries).execute()

  /**
   * Assumes that the tag views table contains no elements for the given persistence ids
   *  Either because tag_views and tag_progress have truncated for this given persistence id
   * or tag writing has never been enabled
   */
  def rebuildTagViewForPersistenceIds(persistenceId: String): Future[Done] =
    new BuildTagViewForPersisetceId(persistenceId, system, recovery, pluginSettings).reconcile()

  /**
   * Returns all the tags in the journal. This is not an efficient query for Cassandra so it is better
   * to calculate tags for calls to deleteTagViewForPersistenceId another way.
   *
   * Prefer to do batches of persistence ids at a time getting the tags just for that persistence id.
   */
  def allTags(): Source[String, NotUsed] = new AllTags(recSession).execute()

  /**
   * Select all the tags for the given persistence id. This may not return tags that
   * have just been used for the first time.
   */
  def tagsForPersistenceId(persistenceId: String): Future[Set[String]] =
    recSession.selectTagProgress(persistenceId).runWith(Sink.seq).map(_.toSet)

  /**
   * Truncate all tables and all metadata so that it can be rebuilt
   */
  def truncateTagView(): Future[Done] = recSession.truncateAll()

}
