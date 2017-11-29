/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.persistence.cassandra.journal.TagWriters.TagWritersSession
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors.Extractor
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.RawEvent
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.PersistenceQuery
import akka.serialization.Serialization
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.{ Done, NotUsed }
import com.datastax.driver.core.Row

import scala.concurrent.Future

object EventsByTagMigration {
  def apply(system: ActorSystem): EventsByTagMigration = new EventsByTagMigration(system)
  // Extracts a Cassandra Row, assuming the pre 0.80 schema into a [[RawEvent]]
  val RawPayloadOldTagSchema: Extractor[RawEvent] = (row: Row, _: EventDeserializer, _: Serialization) => {
    val tags: Set[String] = (1 to 3).foldLeft(Set.empty[String]) {
      case (acc, i) =>
        if (row.getColumnDefinitions.contains(s"tag$i")) {
          val tag = row.getString(s"tag$i")
          if (tag != null) acc + tag
          else acc
        } else {
          acc
        }
    }
    val timeUuid = row.getUUID("timestamp")
    // TODO could cache this
    val meta = if (row.getColumnDefinitions.contains("meta")) {
      Some(SerializedMeta(row.getBytes("meta"), row.getString("meta_ser_manifest"), row.getInt("meta_ser_id")))
    } else {
      None
    }
    RawEvent(
      row.getLong("sequence_nr"),
      Serialized(
        row.getString("persistence_id"),
        row.getLong("sequence_nr"),
        row.getBytes("event"),
        tags,
        row.getString("event_manifest"),
        row.getString("ser_manifest"),
        row.getInt("ser_id"),
        row.getString("writer_uuid"),
        meta,
        timeUuid,
        timeBucket = TimeBucket(timeUuid, Hour) // FIXME, make configurable for migration or just pick it up from normal config
      )
    )
  }
}

class EventsByTagMigration(system: ActorSystem)
  extends CassandraStatements
  with TaggedPreparedStatements
  with CassandraTagRecovery {

  protected val log = Logging.getLogger(system, getClass)
  private val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  private implicit val materialiser = ActorMaterializer()(system)

  implicit val ec = system.dispatcher
  override def config: CassandraJournalConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  val session: CassandraSession = new CassandraSession(
    system,
    config.sessionProvider,
    config.sessionSettings,
    ec,
    log,
    "EventsByTagMigration",
    init = _ => Future.successful(Done)
  )
  private val tagWriters = system.actorOf(TagWriters.props(
    TagWritersSession(
      preparedWriteToTagViewWithoutMeta,
      preparedWriteToTagViewWithMeta,
      session.executeWrite,
      session.selectResultSet,
      preparedWriteToTagProgress
    ),
    config.tagWriterSettings
  ))

  def createTables(): Future[Done] = {
    log.info("Creating keyspace and tables")
    for {
      _ <- session.executeWrite(createKeyspace)
      _ <- session.executeWrite(createTagsTable)
      _ <- session.executeWrite(createTagsProgressTable)
    } yield Done
  }

  def addTagsColumn(): Future[Done] = {
    for {
      _ <- session.executeWrite(s"ALTER TABLE ${tableName} ADD tags set<text>")
    } yield Done
  }

  // TODO offer a version that takes in the persistenceIds in case user has a more efficient way
  // of getting them
  // TODO might be nice to return a summary of what was done rather than just Done
  // TODO add a final flush to the tag writers
  def migrateToTagViews(): Future[Done] = {
    log.info("Beginning migration of data to tag_views table")
    val allPids: Source[RawEvent, NotUsed] = queries.currentPersistenceIds()
      .map { pids =>
        log.info("Migrating the following persistence ids {}", pids)
        pids
      }
      .flatMapConcat(pid => {

        val prereqs: Future[(Map[Tag, TagProgress], SequenceNr)] = for {
          tp <- lookupTagProgress(pid)
          startingSeq = calculateStartingSequenceNr(tp)
        } yield (tp, startingSeq)

        Source.fromFutureSource {
          prereqs.map {
            case (tp, startingSeq) => {
              log.info("Starting migration for pid: {} based on progress: {} starting at sequence nr: {}", pid, tp, startingSeq)
              queries.eventsByPersistenceId[RawEvent](
                pid,
                startingSeq,
                Long.MaxValue,
                Long.MaxValue,
                250,
                None,
                s"migrateToTag-$pid",
                extractor = EventsByTagMigration.RawPayloadOldTagSchema
              ).map(sendMissingTagWriteRaw(tp, tagWriters))
            }
          }
        }
      })

    allPids.runWith(Sink.ignore)
  }
}
