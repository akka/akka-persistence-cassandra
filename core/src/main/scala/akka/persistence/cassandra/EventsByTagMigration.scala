/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import scala.concurrent.ExecutionContext

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.persistence.cassandra.journal.TagWriters.{ AllFlushed, FlushAllTagWriters, TagWritersSession }
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.RawEvent
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors.Extractor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.PersistenceQuery
import akka.serialization.SerializationExtension
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.Timeout
import akka.{ Done, NotUsed }
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.protocol.internal.util.Bytes
import scala.concurrent.Future
import scala.concurrent.duration._

object EventsByTagMigration {
  def apply(system: ActorSystem): EventsByTagMigration =
    new EventsByTagMigration(system)

  // Extracts a Cassandra Row, assuming the pre 0.80 schema into a [[RawEvent]]
  def rawPayloadOldTagSchemaExtractor(
      bucketSize: BucketSize,
      ed: EventDeserializer,
      system: ActorSystem): Extractor[RawEvent] =
    new Extractor[RawEvent](ed, SerializationExtension(system)) {
      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[RawEvent] = {
        // Get the tags from the old location i.e. tag1, tag2, tag3
        val tags: Set[String] =
          if (ed.columnDefinitionCache.hasOldTagsColumns(row)) {
            (1 to 3).foldLeft(Set.empty[String]) {
              case (acc, i) =>
                val tag = row.getString(s"tag$i")
                if (tag != null) {
                  acc + tag
                } else acc
            }
          } else {
            Set.empty
          }

        val timeUuid = row.getUuid("timestamp")
        val sequenceNr = row.getLong("sequence_nr")
        val meta = if (ed.columnDefinitionCache.hasMetaColumns(row)) {
          val m = row.getByteBuffer("meta")
          Option(m).map(SerializedMeta(_, row.getString("meta_ser_manifest"), row.getInt("meta_ser_id")))
        } else {
          None
        }

        def deserializeEvent(): Future[RawEvent] = {
          Future.successful(
            RawEvent(
              sequenceNr,
              Serialized(
                row.getString("persistence_id"),
                row.getLong("sequence_nr"),
                row.getByteBuffer("event"),
                tags,
                row.getString("event_manifest"),
                row.getString("ser_manifest"),
                row.getInt("ser_id"),
                row.getString("writer_uuid"),
                meta,
                timeUuid,
                timeBucket = TimeBucket(timeUuid, bucketSize))))
        }

        if (eventDeserializer.columnDefinitionCache.hasMessageColumn(row)) {
          row.getByteBuffer("message") match {
            case null  => deserializeEvent()
            case bytes =>
              // This is an event from version 0.6 and earlier that used to serialise the PersistentRepr in the
              // message column rather than the event column
              val pr = serialization.deserialize(Bytes.getArray(bytes), classOf[PersistentRepr]).get
              serializeEvent(pr, tags, timeUuid, bucketSize, serialization, system).map { serEvent =>
                RawEvent(sequenceNr, serEvent)
              }
          }
        } else {
          deserializeEvent()
        }
      }
    }

}

/**
 *
 * @param pluginConfigPath The config namespace where the plugin is configured, default is `akka.persistence.cassandra`
 */
class EventsByTagMigration(system: ActorSystem, pluginConfigPath: String = "akka.persistence.cassandra") {
  private[akka] val log = Logging.getLogger(system, getClass)
  private lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](pluginConfigPath + ".query")
  private implicit val sys: ActorSystem = system

  implicit val ec =
    system.dispatchers.lookup(system.settings.config.getString(s"$pluginConfigPath.journal.plugin-dispatcher"))
  private val settings: PluginSettings =
    new PluginSettings(system, system.settings.config.getConfig(pluginConfigPath))
  private val journalStatements = new CassandraJournalStatements(settings)
  private val taggedPreparedStatements = new TaggedPreparedStatements(journalStatements, session.prepare)
  private val tagRecovery =
    new CassandraTagRecovery(system, session, settings, taggedPreparedStatements)

  import journalStatements._

  private def journalSettings = settings.journalSettings
  private def eventsByTagSettings = settings.eventsByTagSettings

  lazy val session: CassandraSession = queries.session

  def createTables(): Future[Done] = {
    log.info("Creating keyspace {} and new tag tables", journalSettings.keyspace)
    for {
      _ <- session.executeWrite(createKeyspace)
      _ <- session.executeWrite(createTagsTable)
      _ <- session.executeWrite(createTagsProgressTable)
      _ <- session.executeWrite(createTagScanningTable)
    } yield Done
  }

  def addTagsColumn(): Future[Done] = {
    log.info("Adding tags column to tabe {}", journalSettings.table)
    for {
      _ <- session.executeWrite(s"ALTER TABLE ${journalSettings.keyspace}.${journalSettings.table} ADD tags set<text>")
    } yield Done
  }

  // TODO might be nice to return a summary of what was done rather than just Done

  /**
   * Migrates the given persistenceIds from the `messages` table to the
   * new `tags_view` table. `tag_view` table must exist before calling this
   * and can be created manually or via [createTagsTable]
   *
   * This is useful if there there is a more efficient way of getting all the
   * persistenceIds than [CassandraReadJournal.currentPersistenceIds] which does
   * a distinct query on the `messages` table.
   *
   * This can also be used to do partial migrations e.g. test a persistenceId in
   * production before migrating everything.
   *
   * It is recommended you use this if the `messages` table is large.
   *
   * @param pids PersistenceIds to migrate
   * @return A Future that completes when the migration is complete
   */
  def migratePidsToTagViews(
      pids: Seq[PersistenceId],
      periodicFlush: Int = 1000,
      flushTimeout: Timeout = Timeout(30.seconds)): Future[Done] = {
    migrateToTagViewsInternal(Source.fromIterator(() => pids.iterator), periodicFlush, flushTimeout)
  }

  /**
   * Migrates the entire `messages` table to the the new `tag_views` table.
   *
   * Uses [CassandraReadJournal.currentPersistenceIds] to find all persistenceIds.
   * Note that this is a very inefficient cassandra query so might timeout. If so
   * the version of this method can be used where the persistenceIds are provided.
   *
   * Persistence ids can be excluded (e.g. useful if you know certain persistenceIds
   * don't use tags
   *
   * @return A Future that completes when the migration is complete.
   */
  def migrateToTagViews(
      periodicFlush: Int = 1000,
      filter: String => Boolean = _ => true,
      flushTimeout: Timeout = Timeout(30.seconds)): Future[Done] = {
    migrateToTagViewsInternal(queries.currentPersistenceIds().filter(filter), periodicFlush, flushTimeout)
  }

  private def migrateToTagViewsInternal(
      src: Source[PersistenceId, NotUsed],
      periodicFlush: Int,
      flushTimeout: Timeout): Future[Done] = {
    log.info("Beginning migration of data to tag_views table in keyspace {}", journalSettings.keyspace)
    val tagWriterSession =
      TagWritersSession(session, journalSettings.writeProfile, journalSettings.readProfile, taggedPreparedStatements)
    val tagWriters = system.actorOf(TagWriters.props(eventsByTagSettings.tagWriterSettings, tagWriterSession))

    val eventDeserializer: CassandraJournal.EventDeserializer =
      new CassandraJournal.EventDeserializer(system)

    implicit val timeout: Timeout = flushTimeout

    val allPids = src
      .map { pids =>
        log.info("Migrating the following persistence ids {}", pids)
        pids
      }
      .flatMapConcat(pid => {
        val prereqs: Future[(Map[Tag, TagProgress], SequenceNr)] = {
          val startingSeqFut = tagRecovery.tagScanningStartingSequenceNr(pid)
          for {
            tp <- tagRecovery.lookupTagProgress(pid)
            _ <- tagRecovery.setTagProgress(pid, tp, tagWriters)
            startingSeq <- startingSeqFut
          } yield (tp, startingSeq)
        }

        // would be nice to group these up into a TagWrites message but also
        // nice that this reuses the recovery code :-/
        Source.futureSource {
          prereqs.map {
            case (tp, startingSeq) => {
              log.info(
                "Starting migration for pid: {} based on progress: {} starting at sequence nr: {}",
                pid,
                tp,
                startingSeq)
              queries
                .eventsByPersistenceId[RawEvent](
                  pid,
                  startingSeq,
                  Long.MaxValue,
                  Long.MaxValue,
                  None,
                  settings.querySettings.readProfile,
                  s"migrateToTag-$pid",
                  extractor = EventsByTagMigration
                    .rawPayloadOldTagSchemaExtractor(eventsByTagSettings.bucketSize, eventDeserializer, system))
                .map(tagRecovery.sendMissingTagWriteRaw(tp, tagWriters, actorRunning = false))
                .buffer(periodicFlush, OverflowStrategy.backpressure)
                .mapAsync(1)(_ => (tagWriters ? FlushAllTagWriters(timeout)).mapTo[AllFlushed.type])
            }
          }
        }
      })

    for {
      _ <- allPids.runWith(Sink.ignore)
      _ <- (tagWriters ? FlushAllTagWriters(timeout)).mapTo[AllFlushed.type]
    } yield Done
  }

}
