/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import java.lang.{ Long => JLong }
import akka.actor.ActorSystem
import akka.persistence.cassandra.PluginSettings
import akka.Done
import akka.persistence.cassandra.journal.CassandraTagRecovery
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.journal.TaggedPreparedStatements
import akka.persistence.cassandra.CassandraStatements
import akka.cassandra.session.scaladsl.CassandraSession
import akka.cassandra.session.scaladsl.CassandraSessionRegistry
import akka.persistence.cassandra.journal.TagWriters._
import akka.persistence.cassandra.journal.TagWriters
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Future
import akka.util.Timeout
import akka.event.Logging
import akka.stream.scaladsl.Source
import akka.actor.ExtendedActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.Extractors
import akka.serialization.SerializationExtension
import akka.stream.OverflowStrategy
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.NotUsed
import akka.persistence.query.NoOffset
import akka.persistence.cassandra.journal.TimeBucket
import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 */
@InternalApi
final class ReconciliationSession(session: CassandraSession, statements: CassandraStatements)(implicit ec: ExecutionContext) {
  private val deleteTagView = session.prepare(statements.journalStatements.deleteTag)

  def deleteFromTagView(tag: String, bucket: TimeBucket, timestamp: Long, persistenceId: String, tagPidSequenceNr: Long): Future[Done] = {
    deleteTagView.flatMap(ps => session.executeWrite(ps.bind(tag, bucket.key: JLong, timestamp: JLong, persistenceId, tagPidSequenceNr: JLong)))
  }
}

/**
 * API likely to change
 */
@ApiMayChange
final class Reconciliation(system: ActorSystem) extends Extension {
  import system.dispatcher

  // TODO make session configurable?
  private val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra", system.dispatcher)
  private val settings = PluginSettings(system)
  private val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  private val statement = new CassandraStatements()
  private val recSession = new ReconciliationPreparedStatements(session, statements)

  def deleteTagViewForPersistenceId(persistenceId: String, tag: String): Future[Done] =
    new DeleteTagViewForPersistenceId(persistenceId, tag, system, session, settings, queries).execute()

  /**
   * Assumes that the tag views table contains no elements for the given persistence ids
   * Either because tag_views and tag_progress have truncated for this given persistence id
   * or tag writing has never been enabled
   */
  def rebuildTagViewForPersistenceIds(persisnteceId: String): Future[Done] = ???

  /**
   * Returns all the tags in the journal. This is not an efficient query for Cassandra so it is better
   * to calculate tags for calls to deleteTagViewForPersistenceId another way.
   */
  def allTags(): Source[String, NotUsed] = ???

  /**
   * Drop tags vies table and all metadata so that it can be rebuilt
   */
  def dropTagViews(): Future[Done] = ???

}

object Reconciliation extends ExtensionId[Reconciliation] with ExtensionIdProvider {

  def createExtension(system: ExtendedActorSystem): Reconciliation = new Reconciliation(system)
  override def lookup(): Reconciliation.type = Reconciliation
  override def get(system: ActorSystem): Reconciliation = super.get(system)
  override def get(system: ClassicActorSystemProvider): Reconciliation = super.get(system)
}



// TODO an extension for this? Java/Scala dsl?

/**
 * Deletes tagged events for a persistence id.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] class DeleteTagViewForPersistenceId(
    persistenceId: String,
    tag: String,
    system: ActorSystem,
    session: CassandraSession,
    settings: PluginSettings,
    queries: CassandraReadJournal) {

  /**
   */
  def execute(): Future[Done] = {
    queries.currentEventsByTagInternal(tag, NoOffset)
      .mapAsync(1) { uuidPr =>
        val tag = tag
        val bucket = TimeBucket(uuidPr.offset, settings.eventsByTagSettings.bucketSize)
        val timestamp = uuidPr.persistentRepr.timestamp
        val persistenceId = uuidPr.persistentRepr.persistenceId
        val tagPidSequenceNr = uuidPr.tagPidSequenceNr
        // TODO issue delete
        //
        //
        ???
      }
    queries.createSourceo()
    queries.currentEventsByTagInternal()
    Future.successful(Done)
  }

}

class BuildTagViewForPersisetceId(
    persistenceId: String,
    system: ActorSystem,
    session: CassandraSession,
    settings: PluginSettings) {

  import system.dispatcher

  val log = Logging(system, classOf[BuildTagViewForPersisetceId])

  // FIXME config path
  private val queries: CassandraReadJournal =
    PersistenceQuery(system.asInstanceOf[ExtendedActorSystem])
      .readJournalFor[CassandraReadJournal]("cassandra-plugin.query")

  private val serialization = SerializationExtension(system)
  private val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)

  private val statements = new CassandraStatements(settings)
  private val tagStatements = new TaggedPreparedStatements(statements.journalStatements, session.prepare)
  private val recovery = new CassandraTagRecovery(system, session, settings, tagStatements)
  private val tagWriterSession: TagWriters.TagWritersSession =
    new TagWritersSession(session, "cassandra-plugin", "cassandra-plugin", tagStatements)
  private val tagWriters =
    system.actorOf(TagWriters.props(settings.eventsByTagSettings.tagWriterSettings, tagWriterSession))

  // FIXME config
  implicit val timeout = Timeout(5.seconds)

  def reconcile(flushEvery: Int = 1000): Future[Done] = {

    val recoveryPrep = for {
      tp <- recovery.lookupTagProgress(persistenceId)
      _ <- recovery.setTagProgress(persistenceId, tp, tagWriters)
    } yield tp

    val what = Source.fromFutureSource(recoveryPrep.map((tp: Map[String, TagProgress]) => {
      log.debug("[{}] Rebuilding tag view table from: [{}]", persistenceId, tp)
      queries
        .eventsByPersistenceId(
          persistenceId,
          0,
          Long.MaxValue,
          Long.MaxValue,
          None,
          settings.journalSettings.readProfile,
          "BuildTagViewForPersistenceId",
          extractor = Extractors.rawEvent(settings.eventsByTagSettings.bucketSize))
        .map(recovery.sendMissingTagWriteRaw(tp, tagWriters, actorRunning = false))
        .buffer(flushEvery, OverflowStrategy.backpressure)
        .mapAsync(1)(_ => (tagWriters ? FlushAllTagWriters(timeout)).mapTo[AllFlushed.type])
    }))

    Future.successful(Done)
  }

}
