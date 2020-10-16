/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query.scaladsl

import java.net.URLEncoder
import java.util.UUID

import akka.{ Done, NotUsed }
import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraJournal.{ PersistenceId, Tag, TagPidSequenceNr }
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.Extractors
import akka.persistence.cassandra.Extractors.Extractor
import akka.persistence.cassandra.query.EventsByTagStage.TagStageSession
import akka.persistence.cassandra.query._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.EventByTagStatements
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.persistence.{ Persistence, PersistentRepr }
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.ActorAttributes
import akka.util.ByteString
import com.datastax.oss.driver.api.core.cql._
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.CassandraStatements
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.journal.CassandraJournal.DeserializedEvent
import akka.serialization.SerializationExtension
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids

object CassandraReadJournal {

  /**
   * The default identifier for [[CassandraReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"akka.persistence.cassandra.query"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = "akka.persistence.cassandra.query"

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case class CombinedEventsByPersistenceIdStmts(
      preparedSelectEventsByPersistenceId: PreparedStatement,
      prepareSelectHighestNr: PreparedStatement,
      preparedSelectDeletedTo: PreparedStatement)

  @InternalApi private[akka] case class EventByTagStatements(byTagWithUpperLimit: PreparedStatement)

  // shared config is one level above the query plugin specific
  private def sharedConfigPath(system: ExtendedActorSystem, queryConfigPath: String): String =
    queryConfigPath.replaceAll("""\.query$""", "")

  // shared config is one level above the query plugin specific
  private def sharedConfig(system: ExtendedActorSystem, queryConfigPath: String): Config =
    system.settings.config.getConfig(sharedConfigPath(system, queryConfigPath))
}

/**
 * Scala API `akka.persistence.query.scaladsl.ReadJournal` implementation for Cassandra.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.cassandra.query.javadsl.CassandraReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"akka.persistence.cassandra.query"`
 * for the default [[CassandraReadJournal#Identifier]]. See `reference.conf`.
 */
class CassandraReadJournal protected (
    system: ExtendedActorSystem,
    sharedConfig: Config,
    sharedConfigPath: String,
    viaNormalConstructor: Boolean)
    extends ReadJournal
    with PersistenceIdsQuery
    with CurrentPersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {

  // This is the constructor that will be used when creating the instance from config
  def this(system: ExtendedActorSystem, cfg: Config, cfgPath: String) =
    this(
      system,
      CassandraReadJournal.sharedConfig(system, cfgPath),
      CassandraReadJournal.sharedConfigPath(system, cfgPath),
      viaNormalConstructor = true)

  import CassandraReadJournal.CombinedEventsByPersistenceIdStmts

  private val log = Logging.getLogger(system, getClass)

  private val settings = new PluginSettings(system, sharedConfig)
  private val statements = new CassandraStatements(settings)

  import settings.querySettings
  import settings.eventsByTagSettings

  if (eventsByTagSettings.eventualConsistency < 1.seconds) {
    log.warning(
      "EventsByTag eventual consistency set below 1 second. This is likely to result in missed events. See reference.conf for details.")
  } else if (eventsByTagSettings.eventualConsistency < 2.seconds) {
    log.info(
      "EventsByTag eventual consistency set below 2 seconds. This can result in missed events. See reference.conf for details.")
  }
  // event adapters are defined in the write section
  private val eventAdapters = Persistence(system).adaptersFor(s"$sharedConfigPath.journal")

  // The EventDeserializer is caching some things based on the column structure and
  // therefore different instances must be used for the eventsByPersistenceId and eventsByTag
  // queries, since the messages table might have a different structure than the tag view.
  private val eventsByPersistenceIdDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)
  private val eventsByTagDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)

  private val serialization = SerializationExtension(system)
  implicit private val ec =
    system.dispatchers.lookup(querySettings.pluginDispatcher)
  implicit private val sys: ActorSystem = system

  private val queryStatements: CassandraReadStatements =
    new CassandraReadStatements {
      override def settings = CassandraReadJournal.this.settings
    }

  /**
   * Data Access Object for arbitrary queries or updates.
   */
  val session: CassandraSession = {
    CassandraSessionRegistry(system).sessionFor(
      CassandraSessionSettings(sharedConfigPath, ses => statements.executeAllCreateKeyspaceAndTables(ses, log)),
      sharedConfig)
  }

  /**
   * Initialize connection to Cassandra and prepared statements.
   * It is not required to do this and it will happen lazily otherwise.
   * It is also not required to wait until this Future is complete to start
   * using the read journal.
   */
  def initialize(): Future[Done] =
    Future
      .sequence(
        List(
          preparedSelectDeletedTo,
          preparedSelectAllPersistenceIds,
          preparedSelectEventsByPersistenceId,
          preparedSelectFromTagViewWithUpperBound,
          preparedSelectTagSequenceNrs))
      .map(_ => Done)

  private def preparedSelectEventsByPersistenceId: Future[PreparedStatement] =
    session.prepare(statements.journalStatements.selectMessages)

  private def preparedSelectDeletedTo: Future[PreparedStatement] =
    session.prepare(statements.journalStatements.selectDeletedTo)

  private def preparedSelectAllPersistenceIds: Future[PreparedStatement] =
    session.prepare(queryStatements.selectAllPersistenceIds)

  private def preparedSelectDistinctPersistenceIds: Future[PreparedStatement] =
    session.prepare(queryStatements.selectDistinctPersistenceIds)

  private def preparedSelectFromTagViewWithUpperBound: Future[PreparedStatement] =
    session.prepare(queryStatements.selectEventsFromTagViewWithUpperBound)

  private def preparedSelectTagSequenceNrs: Future[PreparedStatement] =
    session.prepare(queryStatements.selectTagSequenceNrs)

  private def preparedSelectHighestSequenceNr: Future[PreparedStatement] =
    session.prepare(statements.journalStatements.selectHighestSequenceNr)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def combinedEventsByPersistenceIdStmts: Future[CombinedEventsByPersistenceIdStmts] =
    for {
      ps1 <- preparedSelectEventsByPersistenceId
      ps2 <- preparedSelectHighestSequenceNr
      ps3 <- preparedSelectDeletedTo
    } yield CombinedEventsByPersistenceIdStmts(ps1, ps2, ps3)

  /** INTERNAL API */
  @InternalApi private[akka] def combinedEventsByTagStmts: Future[EventByTagStatements] =
    for {
      byTagWithUpper <- preparedSelectFromTagViewWithUpperBound
    } yield EventByTagStatements(byTagWithUpper)

  /**
   * Use this as the UUID offset in `eventsByTag` queries when you want all
   * events from the beginning of time.
   */
  val firstOffset: UUID = {
    val timestamp = eventsByTagSettings.firstTimeBucket.key
    Uuids.startOf(timestamp)
  }

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`).
   */
  def offsetUuid(timestamp: Long): UUID =
    if (timestamp == 0L) firstOffset else Uuids.startOf(timestamp)

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`).
   */
  def timeBasedUUIDFrom(timestamp: Long): Offset =
    if (timestamp == 0L) NoOffset
    else TimeBasedUUID(offsetUuid(timestamp))

  /**
   * Convert a `TimeBasedUUID` to a unix timestamp (as returned by
   * `System#currentTimeMillis`).
   */
  def timestampFrom(offset: TimeBasedUUID): Long =
    Uuids.unixTimestamp(offset.value)

  /**
   * Convert a `TimeBasedUUID` to a unix timestamp (as returned by
   * `System#currentTimeMillis`. If it's not a `TimeBasedUUID` it
   * will return 0.
   */
  private def timestampFrom(offset: Offset): Long =
    offset match {
      case t: TimeBasedUUID => timestampFrom(t)
      case _                => 0
    }

  /**
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an `akka.persistence.journal.EventAdapter` that wraps the events
   * in a `akka.persistence.journal.Tagged` with the given `tags`.
   * The tags must be defined in the `tags` section of the `akka.persistence.cassandra` configuration.
   *
   * You can use [[NoOffset]] to retrieve all events with a given tag or
   * retrieve a subset of all events by specifying a `TimeBasedUUID` `offset`.
   *
   * The offset of each event is provided in the streamed envelopes returned,
   * which makes it possible to resume the stream at a later point from a given offset.
   * The `offset` parameter is exclusive, i.e. the event corresponding to the given `offset` parameter is not
   * included in the stream. The `Offset` type is `akka.persistence.query.TimeBasedUUID`.
   *
   * For querying events that happened after a long unix timestamp you can use [[timeBasedUUIDFrom]]
   * to create the offset to use with this method.
   *
   * In addition to the `offset` the envelope also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * The returned event stream is ordered by the offset (timestamp), which corresponds
   * to the same order as the write journal stored the events, with inaccuracy due to clock skew
   * between different nodes. The same stream elements (in same order) are returned for multiple
   * executions of the query on a best effort basis. The query is using a batched writes to a
   * separate table so is eventually consistent.
   * This means that different queries may see different
   * events for the latest events, but eventually the result will be ordered by timestamp
   * (Cassandra timeuuid column).
   *
   * However a strong guarantee is provided that events for a given persistenceId will
   * be delivered in order, the eventual consistency is only for ordering of events
   * from different persistenceIds.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by [[currentEventsByTag]].
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    eventsByTagInternal(tag, offset)
      .mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
      .mapMaterializedValue(_ => NotUsed)
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def eventsByTagInternal(tag: String, offset: Offset): Source[UUIDPersistentRepr, NotUsed] =
    if (!eventsByTagSettings.eventsByTagEnabled)
      Source.failed(
        new IllegalStateException(
          "Events by tag queries are disabled with configuration " +
          "events-by-tag.enabled=off"))
    else {
      try {
        val (fromOffset, usingOffset) = offsetToInternalOffset(offset)
        val prereqs = eventsByTagPrereqs(tag, usingOffset, fromOffset)
        createFutureSource(prereqs) {
          case (s, (statements, initialTagPidSequenceNrs, scanner)) =>
            val session =
              new TagStageSession(tag, querySettings.readProfile, s, statements, eventsByTagSettings.retrySettings)
            Source.fromGraph(
              EventsByTagStage(
                session,
                fromOffset,
                None,
                settings,
                Some(querySettings.refreshInterval),
                eventsByTagSettings.bucketSize,
                usingOffset,
                initialTagPidSequenceNrs,
                scanner))
        }.via(deserializeEventsByTagRow).mapMaterializedValue(_ => NotUsed)

      } catch {
        case NonFatal(e) =>
          // e.g. from cassandraSession, or selectStatement
          log.debug("Could not run eventsByTag [{}] query, due to: {}", tag, e.getMessage)
          Source.failed(e)
      }
    }

  private def deserializeEventsByTagRow: Flow[EventsByTagStage.UUIDRow, UUIDPersistentRepr, NotUsed] = {
    val deserializeEventAsync = querySettings.deserializationParallelism > 1
    Flow[EventsByTagStage.UUIDRow]
      .mapAsync(querySettings.deserializationParallelism) { uuidRow =>
        val row = uuidRow.row
        eventsByTagDeserializer.deserializeEvent(row, deserializeEventAsync).map {
          case DeserializedEvent(payload, metadata) =>
            val repr = mapEvent(PersistentRepr(
              payload,
              sequenceNr = uuidRow.sequenceNr,
              persistenceId = uuidRow.persistenceId,
              manifest = row.getString("event_manifest"),
              deleted = false,
              sender = null,
              writerUuid = row.getString("writer_uuid")))
            val reprWithMeta = metadata match {
              case OptionVal.None           => repr
              case OptionVal.Some(metadata) => repr.withMetadata(metadata)
            }
            UUIDPersistentRepr(uuidRow.offset, uuidRow.tagPidSequenceNr, reprWithMeta)
        }
      }
      .withAttributes(ActorAttributes.dispatcher(querySettings.pluginDispatcher))
  }

  private def eventsByTagPrereqs(tag: String, usingOffset: Boolean, fromOffset: UUID)
      : Future[(EventByTagStatements, Map[Tag, (TagPidSequenceNr, UUID)], TagViewSequenceNumberScanner)] = {
    val currentBucket =
      TimeBucket(System.currentTimeMillis(), eventsByTagSettings.bucketSize)
    val initialTagPidSequenceNrs =
      if (usingOffset && currentBucket.within(fromOffset) && eventsByTagSettings.offsetScanning > Duration.Zero)
        calculateStartingTagPidSequenceNrs(tag, fromOffset)
      else
        Future.successful(Map.empty[Tag, (TagPidSequenceNr, UUID)])

    for {
      statements <- combinedEventsByTagStmts
      tagSequenceNrs <- initialTagPidSequenceNrs
      tagViewScanner <- tagViewScanner
    } yield (statements, tagSequenceNrs, tagViewScanner)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] val tagViewScanner: Future[TagViewSequenceNumberScanner] = preparedSelectTagSequenceNrs.map { ps =>
    new TagViewSequenceNumberScanner(TagViewSequenceNumberScanner.Session(session, ps, querySettings.readProfile))
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def calculateStartingTagPidSequenceNrs(
      tag: String,
      fromOffset: UUID): Future[Map[PersistenceId, (TagPidSequenceNr, UUID)]] = {
    tagViewScanner.flatMap { scanner =>
      // Subtract 1 so events by tag looks for the lowest tagPidSequenceNumber that was found during initial scanning
      // Make a fake UUID for this tagPidSequenceNr that will be used to search for this tagPidSequenceNr in the unlikely
      // event that the stage can't find the event found during this scan
      scanner
        .scan(
          tag,
          fromOffset,
          Uuids.endOf(System.currentTimeMillis() + eventsByTagSettings.offsetScanning.toMillis),
          eventsByTagSettings.bucketSize,
          eventsByTagSettings.offsetScanning,
          math.min)
        .map { progress =>
          progress.map {
            case (key, (tagPidSequenceNr, uuid)) =>
              val unixTime = Uuids.unixTimestamp(uuid)
              (key, (tagPidSequenceNr - 1, Uuids.startOf(unixTime - 1)))
          }
        }
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createSource[T, P](
      prepStmt: Future[P],
      source: (CqlSession, P) => Source[T, NotUsed]): Source[T, NotUsed] = {
    // when we get the PreparedStatement we know that the session is initialized,
    // i.e.the get is safe
    def getSession: CqlSession = session.underlying().value.get.get

    prepStmt.value match {
      case Some(Success(ps)) => source(getSession, ps)
      case Some(Failure(e))  => Source.failed(e)
      case None              =>
        // completed later
        Source
          .maybe[P]
          .mapMaterializedValue { promise =>
            promise.completeWith(prepStmt.map(Option(_)))
            NotUsed
          }
          .flatMapConcat(ps => source(getSession, ps))
    }

  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createFutureSource[T, P, M](prepStmt: Future[P])(
      source: (CqlSession, P) => Source[T, M]): Source[T, Future[M]] = {
    // when we get the PreparedStatement we know that the session is initialized,
    // i.e.the get is safe
    def getSession: CqlSession = session.underlying().value.get.get

    prepStmt.value match {
      case Some(Success(ps)) =>
        source(getSession, ps).mapMaterializedValue(Future.successful)
      case Some(Failure(e)) =>
        Source.failed(e).mapMaterializedValue(_ => Future.failed(e))
      case None =>
        // completed later
        Source.futureSource(prepStmt.map(ps => source(getSession, ps)))
    }

  }

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   *
   * Use `NoOffset` when you want all events from the beginning of time.
   * To acquire an offset from a long unix timestamp to use with this query, you can use [[timeBasedUUIDFrom]].
   *
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    currentEventsByTagInternal(tag, offset)
      .mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def currentEventsByTagInternal(
      tag: String,
      offset: Offset): Source[UUIDPersistentRepr, NotUsed] =
    if (!eventsByTagSettings.eventsByTagEnabled)
      Source.failed(new IllegalStateException("Events by tag queries are disabled"))
    else {
      try {
        val (fromOffset, usingOffset) = offsetToInternalOffset(offset)
        val prereqs = eventsByTagPrereqs(tag, usingOffset, fromOffset)
        // pick up all the events written this millisecond
        val toOffset = Some(Uuids.endOf(System.currentTimeMillis()))

        createFutureSource(prereqs) {
          case (s, (statements, initialTagPidSequenceNrs, scanner)) =>
            val session =
              new TagStageSession(tag, querySettings.readProfile, s, statements, eventsByTagSettings.retrySettings)
            Source.fromGraph(
              EventsByTagStage(
                session,
                fromOffset,
                toOffset,
                settings,
                None,
                eventsByTagSettings.bucketSize,
                usingOffset,
                initialTagPidSequenceNrs,
                scanner))
        }.via(deserializeEventsByTagRow).mapMaterializedValue(_ => NotUsed)

      } catch {
        case NonFatal(e) =>
          // e.g. from cassandraSession, or selectStatement
          log.debug("Could not run currentEventsByTag [{}] query, due to: {}", tag, e.getMessage)
          Source.failed(e)
      }
    }

  /**
   * `eventsByPersistenceId` is used to retrieve a stream of events for a particular persistenceId.
   *
   * The `EventEnvelope` contains the event and provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * `fromSequenceNr` and `toSequenceNr` can be specified to limit the set of returned events.
   * The `fromSequenceNr` and `toSequenceNr` are inclusive.
   *
   * The `EventEnvelope` also provides an `offset`, which is the same kind of offset as is used in the
   * `eventsByTag` query. The `Offset` type is `akka.persistence.query.TimeBasedUUID`.
   *
   * The returned event stream is ordered by `sequenceNr`.
   *
   * Deleted events are also deleted from the event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByPersistenceId`.
   */
  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      Some(querySettings.refreshInterval),
      querySettings.readProfile,
      s"eventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentReprAndOffset(eventsByPersistenceIdDeserializer, serialization))
      .mapConcat {
        case (persistentRepr, offset) => toEventEnvelope(mapEvent(persistentRepr), offset)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      None,
      querySettings.readProfile,
      s"currentEventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentReprAndOffset(eventsByPersistenceIdDeserializer, serialization))
      .mapConcat {
        case (persistentRepr, offset) => toEventEnvelope(mapEvent(persistentRepr), offset)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def eventsByPersistenceIdWithControl(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      refreshInterval: Option[FiniteDuration] = None)
      : Source[EventEnvelope, Future[EventsByPersistenceIdStage.Control]] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      refreshInterval.orElse(Some(querySettings.refreshInterval)),
      settings.journalSettings.readProfile, // write journal read-profile
      s"eventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentReprAndOffset(eventsByPersistenceIdDeserializer, serialization),
      fastForwardEnabled = true).mapConcat {
      case (persistentRepr, offset) => toEventEnvelope(mapEvent(persistentRepr), offset)
    }

  /**
   * INTERNAL API: This is a low-level method that return journal events as they are persisted.
   *
   * The fromJournal adaptation happens at higher level:
   *  - In the AsyncWriteJournal for the PersistentActor and PersistentView recovery.
   *  - In the public eventsByPersistenceId and currentEventsByPersistenceId queries.
   */
  @InternalApi private[akka] def eventsByPersistenceId[T](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      refreshInterval: Option[FiniteDuration],
      readProfile: String,
      name: String,
      extractor: Extractor[T],
      fastForwardEnabled: Boolean = false): Source[T, Future[EventsByPersistenceIdStage.Control]] = {

    val deserializeEventAsync = querySettings.deserializationParallelism > 1

    createFutureSource(combinedEventsByPersistenceIdStmts) { (s, c) =>
      log.debug("Creating EventByPersistentIdState graph")
      Source
        .fromGraph(
          new EventsByPersistenceIdStage(
            persistenceId,
            fromSequenceNr,
            toSequenceNr,
            max,
            refreshInterval,
            EventsByPersistenceIdStage.EventsByPersistenceIdSession(
              c.preparedSelectEventsByPersistenceId,
              c.prepareSelectHighestNr,
              c.preparedSelectDeletedTo,
              s,
              querySettings.readProfile),
            settings,
            fastForwardEnabled))
        .withAttributes(ActorAttributes.dispatcher(querySettings.pluginDispatcher))
        .named(name)
    }.mapAsync(querySettings.deserializationParallelism) { row =>
        extractor.extract(row, deserializeEventAsync)
      }
      .withAttributes(ActorAttributes.dispatcher(querySettings.pluginDispatcher))
  }

  /**
   * INTERNAL API: Internal hook for amending the event payload. Called from all queries.
   */
  @InternalApi private[akka] def mapEvent(persistentRepr: PersistentRepr): PersistentRepr =
    persistentRepr

  private def toEventEnvelope(persistentRepr: PersistentRepr, offset: Offset): immutable.Iterable[EventEnvelope] =
    adaptFromJournal(persistentRepr).map { payload =>
      EventEnvelope(
        offset,
        persistentRepr.persistenceId,
        persistentRepr.sequenceNr,
        payload,
        timestampFrom(offset),
        persistentRepr.metadata)
    }

  private def offsetToInternalOffset(offset: Offset): (UUID, Boolean) =
    offset match {
      case TimeBasedUUID(uuid) => (uuid, true)
      case NoOffset            => (firstOffset, false)
      case unsupported =>
        throw new IllegalArgumentException("Cassandra does not support " + unsupported.getClass.getName + " offsets")
    }

  private def adaptFromJournal(persistentRepr: PersistentRepr): immutable.Iterable[Any] = {
    val eventAdapter = eventAdapters.get(persistentRepr.payload.getClass)
    val eventSeq =
      eventAdapter.fromJournal(persistentRepr.payload, persistentRepr.manifest)
    eventSeq.events
  }

  /**
   * `allPersistenceIds` is used to retrieve a stream of `persistenceId`s.
   *
   * The stream emits `persistenceId` strings.
   *
   * The stream guarantees that a `persistenceId` is only emitted once and there are no duplicates.
   * Order is not defined. Multiple executions of the same stream (even bounded) may emit different
   * sequence of `persistenceId`s.
   *
   * The stream is not completed when it reaches the end of the currently known `persistenceId`s,
   * but it continues to push new `persistenceId`s when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * known `persistenceId`s is provided by `currentPersistenceIds`.
   */
  override def persistenceIds(): Source[String, NotUsed] =
    persistenceIds(Some(querySettings.refreshInterval), "allPersistenceIds")

  /**
   * Same type of query as `persistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] =
    persistenceIds(None, "currentPersistenceIds")

  private def persistenceIds(refreshInterval: Option[FiniteDuration], name: String): Source[String, NotUsed] =
    if (!settings.journalSettings.supportAllPersistenceIds)
      Source.failed(
        new IllegalStateException(
          "persistenceIds queries are disabled with configuration " +
          "support-all-persistence-ids=off"))
    else {
      createSource[String, PreparedStatement](
        preparedSelectAllPersistenceIds,
        (s, ps) =>
          Source
            .fromGraph(new AllPersistenceIdsStage(refreshInterval, ps, s, querySettings.readProfile))
            .withAttributes(ActorAttributes.dispatcher(querySettings.pluginDispatcher))
            .mapMaterializedValue(_ => NotUsed)
            .named(name))
    }

  /**
   * INTERNAL API: Needed for migration to 1.0
   */
  @InternalApi private[akka] def currentPersistenceIdsFromMessages(): Source[String, NotUsed] =
    createSource[String, PreparedStatement](
      preparedSelectDistinctPersistenceIds,
      (s, ps) =>
        Source
          .fromGraph(new AllPersistenceIdsStage(None, ps, s, querySettings.readProfile))
          .withAttributes(ActorAttributes.dispatcher(querySettings.pluginDispatcher))
          .mapMaterializedValue(_ => NotUsed)
          .named("currentPersistenceIdsFromMessages"))

}
