/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.pattern.ask
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter.TagProgress
import akka.persistence.cassandra.journal.TagWriters.{ AllFlushed, FlushAllTagWriters, TagWritersSession }
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.RawEvent
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.PersistenceQuery
import akka.serialization.Serialization
import akka.stream.{ ActorMaterializer, OverflowStrategy }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.Timeout
import akka.{ Done, NotUsed }
import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.Bytes

import scala.concurrent.Future
import scala.concurrent.duration._

object EventsByTagMigration {
  def apply(system: ActorSystem): EventsByTagMigration = new EventsByTagMigration(system)
  // Extracts a Cassandra Row, assuming the pre 0.80 schema into a [[RawEvent]]
  val RawPayloadOldTagSchema = (bucketSize: BucketSize, transportInformation: Option[Serialization.Information]) => (row: Row, _: EventDeserializer, serialization: Serialization) => {
    // Get the tags from the old location i.e. tag1, tag2, tag3
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
    val sequenceNr = row.getLong("sequence_nr")
    // TODO could cache this decision
    val meta = if (row.getColumnDefinitions.contains("meta")) {
      val m = row.getBytes("meta")
      Option(m).map(SerializedMeta(_, row.getString("meta_ser_manifest"), row.getInt("meta_ser_id")))
    } else {
      None
    }

    row.getBytes("message") match {
      case null =>
        RawEvent(
          sequenceNr,
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
            timeBucket = TimeBucket(timeUuid, bucketSize)
          )
        )
      case bytes =>
        // This is an event from version 0.7 that used to serialise the PersistentRepr in the
        // message column rather than the event column
        val pr = serialization.deserialize(Bytes.getArray(bytes), classOf[PersistentRepr]).get
        RawEvent(
          sequenceNr,
          serializeEvent(pr, tags, timeUuid, bucketSize, serialization, transportInformation)
        )
    }
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
  def migratePidsToTagViews(pids: Seq[PersistenceId], periodicFlush: Int = 1000): Future[Done] = {
    migrateToTagViewsInternal(Source.fromIterator(() => pids.iterator), periodicFlush)
  }

  /**
   * Migrates the entire `messages` table to the the new `tag_views` table.
   *
   * Uses [CassandraReadJournal.currentPersistenceIds] to find all persistenceIds.
   * Note that this is a very inefficient cassandra query so might timeout. If so
   * the version of this method can be used where the persistenceIds are provided.
   *
   * @return A Future that completes when the migration is complete.
   */
  def migrateToTagViews(periodicFlush: Int = 1000): Future[Done] = {
    migrateToTagViewsInternal(queries.currentPersistenceIds(), periodicFlush)
  }

  private def migrateToTagViewsInternal(src: Source[PersistenceId, NotUsed], periodicFlush: Int): Future[Done] = {
    log.info("Beginning migration of data to tag_views table")
    val tagWriterSession = TagWritersSession(
      preparedWriteToTagViewWithoutMeta,
      preparedWriteToTagViewWithMeta,
      session.executeWrite,
      session.selectResultSet,
      preparedWriteToTagProgress
    )
    val tagWriters = system.actorOf(TagWriters.props(
      (arf, tag) => arf.actorOf(TagWriter.props(tagWriterSession, tag, config.tagWriterSettings))
    ))

    // A bit arbitrary, but we could be waiting on many Cassandra writes during the flush
    implicit val timeout = Timeout(30.seconds)
    val allPids = src
      .map { pids =>
        log.info("Migrating the following persistence ids {}", pids)
        pids
      }.flatMapConcat(pid => {
        val prereqs: Future[(Map[Tag, TagProgress], SequenceNr)] = for {
          tp <- lookupTagProgress(pid)
          _ <- sendTagProgress(tp, tagWriters)
          startingSeq = calculateStartingSequenceNr(tp)
        } yield (tp, startingSeq)

        // would be nice to group these up into a TagWrites message but also
        // nice that this reuses the recovery code :-/
        Source.fromFutureSource {
          prereqs.map {
            case (tp, startingSeq) => {
              log.info("Starting migration for pid: {} based on progress: {} starting at sequence nr: {}", pid, tp, startingSeq)
              queries.eventsByPersistenceId[RawEvent](
                pid,
                startingSeq,
                Long.MaxValue,
                Long.MaxValue,
                config.replayMaxResultSize,
                None,
                s"migrateToTag-$pid",
                extractor = EventsByTagMigration.RawPayloadOldTagSchema(config.bucketSize, transportInformation)
              ).map(sendMissingTagWriteRaw(tp, tagWriters))
                .buffer(periodicFlush, OverflowStrategy.backpressure)
                .mapAsync(1)(_ => (tagWriters ? FlushAllTagWriters).mapTo[AllFlushed.type])
            }
          }
        }
      })

    for {
      _ <- allPids.runWith(Sink.ignore)
      _ <- (tagWriters ? FlushAllTagWriters).mapTo[AllFlushed.type]
    } yield Done
  }

  private lazy val transportInformation: Option[Serialization.Information] = {
    val address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, system))
  }
}
