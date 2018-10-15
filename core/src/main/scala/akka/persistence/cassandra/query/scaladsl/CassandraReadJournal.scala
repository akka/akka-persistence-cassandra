/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query.scaladsl

import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.{ Done, NotUsed }
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.cassandra.journal.CassandraJournal.{ PersistenceId, Tag, TagPidSequenceNr }
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.query.AllPersistenceIdsPublisher.AllPersistenceIdsSession
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors.Extractor
import akka.persistence.cassandra.query.EventsByTagStage.TagStageSession
import akka.persistence.cassandra.query._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.CombinedEventsByTagStmts
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.persistence.{ Persistence, PersistentRepr }
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.{ ActorAttributes, ActorMaterializer }
import akka.util.ByteString
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ LoggingRetryPolicy, RetryPolicy }
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

import akka.serialization.SerializationExtension

object CassandraReadJournal {
  //temporary counter for keeping Read Journal metrics unique
  // TODO: remove after akka/akka#19822 is fixed
  private val InstanceUID = new AtomicLong(-1)
  /**
   * The default identifier for [[CassandraReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"cassandra-query-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = "cassandra-query-journal"

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case class CombinedEventsByPersistenceIdStmts(
    preparedSelectEventsByPersistenceId: PreparedStatement,
    preparedSelectDeletedTo:             PreparedStatement)

  @InternalApi private[akka] case class CombinedEventsByTagStmts(
    byTag:               PreparedStatement,
    byTagWithUpperLimit: PreparedStatement)
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
 * absolute path corresponding to the identifier, which is `"cassandra-query-journal"`
 * for the default [[CassandraReadJournal#Identifier]]. See `reference.conf`.
 */
class CassandraReadJournal(system: ExtendedActorSystem, cfg: Config)
  extends ReadJournal
  with PersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery
  with CassandraStatements {

  import CassandraReadJournal.CombinedEventsByPersistenceIdStmts

  private val log = Logging.getLogger(system, getClass)
  private val writePluginId = cfg.getString("write-plugin")
  private val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig(writePluginId))
  private val queryPluginConfig = new CassandraReadJournalConfig(cfg, writePluginConfig)
  private val eventAdapters = Persistence(system).adaptersFor(writePluginId)

  // The EventDeserializer is caching some things based on the column structure and
  // therefore different instances must be used for the eventsByPersistenceId and eventsByTag
  // queries, since the messages table might have a different structure than the tag view.
  private val eventsByPersistenceIdDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)
  private val eventsByTagDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)

  private val serialization = SerializationExtension(system)
  implicit private val ec = system.dispatchers.lookup(queryPluginConfig.pluginDispatcher)
  implicit private val materializer = ActorMaterializer()(system)

  // TODO: change categoryUid to unique config path after akka/akka#19822 is fixed
  private val metricsCategory = {
    val categoryUid =
      CassandraReadJournal.InstanceUID.incrementAndGet() match {
        case 0     => ""
        case other => other
      }
    s"${CassandraReadJournal.Identifier}$categoryUid"
  }

  private val queryStatements: CassandraReadStatements = new CassandraReadStatements {
    override def config = queryPluginConfig
  }

  def config: CassandraJournalConfig = writePluginConfig

  /**
   * Data Access Object for arbitrary queries or updates.
   */
  val session: CassandraSession = new CassandraSession(
    system,
    writePluginConfig.sessionProvider,
    writePluginConfig.sessionSettings,
    ec,
    log,
    metricsCategory,
    init = session => executeCreateKeyspaceAndTables(session, writePluginConfig))

  /**
   * Initialize connection to Cassandra and prepared statements.
   * It is not required to do this and it will happen lazily otherwise.
   * It is also not required to wait until this Future is complete to start
   * using the read journal.
   */
  def initialize(): Future[Done] = {
    Future.sequence(List(
      preparedSelectDeletedTo,
      preparedSelectDistinctPersistenceIds,
      preparedSelectEventsByPersistenceId,
      preparedSelectFromTagViewWithUpperBound,
      preparedSelectTagSequenceNrs)).map(_ => Done)
  }

  private val readRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(queryPluginConfig.readRetries))

  private def preparedSelectEventsByPersistenceId: Future[PreparedStatement] =
    session
      .prepare(selectMessages)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true)
        .setRetryPolicy(readRetryPolicy))

  private def preparedSelectDeletedTo: Future[PreparedStatement] =
    session
      .prepare(selectDeletedTo)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true)
        .setRetryPolicy(readRetryPolicy))

  private def preparedSelectDistinctPersistenceIds: Future[PreparedStatement] =
    session
      .prepare(queryStatements.selectDistinctPersistenceIds)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true)
        .setRetryPolicy(readRetryPolicy))

  private def prepareSelectFromTagView: Future[PreparedStatement] =
    session
      .prepare(queryStatements.selectEventsFromTagView)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency)
        .setIdempotent(true).setRetryPolicy(readRetryPolicy))

  private def preparedSelectFromTagViewWithUpperBound: Future[PreparedStatement] =
    session
      .prepare(queryStatements.selectEventsFromTagViewWithUpperBound)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true)
        .setRetryPolicy(readRetryPolicy))

  private def preparedSelectTagSequenceNrs: Future[PreparedStatement] =
    session.prepare(queryStatements.selectTagSequenceNrs)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true)
        .setRetryPolicy(readRetryPolicy))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def combinedEventsByPersistenceIdStmts: Future[CombinedEventsByPersistenceIdStmts] =
    for {
      ps1 <- preparedSelectEventsByPersistenceId
      ps2 <- preparedSelectDeletedTo
    } yield CombinedEventsByPersistenceIdStmts(ps1, ps2)

  @InternalApi private[akka] def combinedEventsByTagStmts: Future[CombinedEventsByTagStmts] =
    for {
      byTag <- prepareSelectFromTagView
      byTagWithUpper <- preparedSelectFromTagViewWithUpperBound
    } yield CombinedEventsByTagStmts(byTag, byTagWithUpper)

  system.registerOnTermination {
    session.close()
  }

  /**
   * Use this as the UUID offset in `eventsByTag` queries when you want all
   * events from the beginning of time.
   */
  val firstOffset: UUID = {
    // FIXME perhaps we can do something smarter, such as caching the highest offset retrieved
    // from queries
    val timestamp = queryPluginConfig.firstTimeBucket.key
    UUIDs.startOf(timestamp)
  }

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def offsetUuid(timestamp: Long): UUID =
    if (timestamp == 0L) firstOffset else UUIDs.startOf(timestamp)

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def timeBasedUUIDFrom(timestamp: Long): Offset = {
    if (timestamp == 0L) NoOffset
    else TimeBasedUUID(offsetUuid(timestamp))
  }

  /**
   * Convert a `TimeBasedUUID` to a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def timestampFrom(offset: TimeBasedUUID): Long =
    UUIDs.unixTimestamp(offset.value)

  /**
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an `akka.persistence.journal.EventAdapter` that wraps the events
   * in a `akka.persistence.journal.Tagged` with the given `tags`.
   * The tags must be defined in the `tags` section of the `cassandra-journal` configuration.
   *
   * You can use [[NoOffset]] to retrieve all events with a given tag or
   * retrieve a subset of all events by specifying a `TimeBasedUUID` `offset`.
   *
   * The offset of each event is provided in the streamed envelopes returned,
   * which makes it possible to resume the stream at a later point from a given offset.
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
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    eventsByTagInternal(tag, offset).mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
      .mapMaterializedValue(_ => NotUsed)
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))

  }

  private[akka] def eventsByTagInternal(tag: String, offset: Offset): Source[UUIDPersistentRepr, NotUsed] = {
    if (!config.eventsByTagEnabled)
      Source.failed(new IllegalStateException("Events by tag queries are disabled"))
    else {
      try {
        val (fromOffset, usingOffset) = offsetToInternalOffset(offset)
        val prereqs = eventsByTagPrereqs(tag, usingOffset, fromOffset)
        createFutureSource(prereqs) { (s, prereqs) =>
          val session = TagStageSession(tag, s, prereqs._1, queryPluginConfig.fetchSize)
          Source.fromGraph(EventsByTagStage(
            session, fromOffset,
            None, queryPluginConfig,
            Some(queryPluginConfig.refreshInterval),
            writePluginConfig.bucketSize,
            usingOffset,
            prereqs._2))
        }
          .via(deserializeEventsByTagRow)
          .mapMaterializedValue(_ => NotUsed)

      } catch {
        case NonFatal(e) =>
          // e.g. from cassandraSession, or selectStatement
          log.debug("Could not run eventsByTag [{}] query, due to: {}", tag, e.getMessage)
          Source.failed(e)
      }
    }
  }

  private def deserializeEventsByTagRow: Flow[EventsByTagStage.UUIDRow, UUIDPersistentRepr, NotUsed] = {
    val deserializeEventAsync = queryPluginConfig.deserializationParallelism > 1
    Flow[EventsByTagStage.UUIDRow]
      .mapAsync(queryPluginConfig.deserializationParallelism) { uuidRow =>
        val row = uuidRow.row
        eventsByTagDeserializer.deserializeEvent(row, deserializeEventAsync).map { payload =>
          val repr = mapEvent(PersistentRepr(
            payload,
            sequenceNr = uuidRow.sequenceNr,
            persistenceId = uuidRow.persistenceId,
            manifest = row.getString("event_manifest"),
            deleted = false,
            sender = null,
            writerUuid = row.getString("writer_uuid")))
          UUIDPersistentRepr(uuidRow.offset, uuidRow.tagPidSequenceNr, repr)
        }
      }
      .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
  }

  private def eventsByTagPrereqs(
    tag:         String,
    usingOffset: Boolean,
    fromOffset:  UUID): Future[(CombinedEventsByTagStmts, Map[Tag, (TagPidSequenceNr, UUID)])] = {
    val currentBucket = TimeBucket(System.currentTimeMillis(), writePluginConfig.bucketSize)
    val initialTagPidSequenceNrs = if (usingOffset && currentBucket.within(fromOffset))
      scanTagSequenceNrs(tag, fromOffset)
    else
      Future.successful(Map.empty[Tag, (TagPidSequenceNr, UUID)])

    for {
      statements <- combinedEventsByTagStmts
      tagSequenceNrs <- initialTagPidSequenceNrs
    } yield (statements, tagSequenceNrs)
  }

  private[akka] def scanTagSequenceNrs(tag: String, fromOffset: UUID): Future[Map[PersistenceId, (TagPidSequenceNr, UUID)]] = {
    val scanner = new TagViewSequenceNumberScanner(session, preparedSelectTagSequenceNrs)
    val bucket = TimeBucket(fromOffset, writePluginConfig.bucketSize)
    scanner.scan(tag, fromOffset, bucket, queryPluginConfig.eventsByTagOffsetScanning)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createSource[T, P](
    prepStmt: Future[P],
    source:   (Session, P) => Source[T, NotUsed]): Source[T, NotUsed] = {
    // when we get the PreparedStatement we know that the session is initialized,
    // i.e.the get is safe
    def getSession: Session = session.underlying().value.get.get

    prepStmt.value match {
      case Some(Success(ps)) => source(getSession, ps)
      case Some(Failure(e))  => Source.failed(e)
      case None =>
        // completed later
        Source.maybe[P]
          .mapMaterializedValue { promise =>
            promise.completeWith(prepStmt.map(Option(_)))
            NotUsed
          }
          .flatMapConcat(ps => source(getSession, ps))
    }

  }

  // FIXME perhaps use createFutureSource in all places

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createFutureSource[T, P, M](
    prepStmt: Future[P])(source: (Session, P) => Source[T, M]): Source[T, Future[M]] = {
    // when we get the PreparedStatement we know that the session is initialized,
    // i.e.the get is safe
    def getSession: Session = session.underlying().value.get.get

    prepStmt.value match {
      case Some(Success(ps)) =>
        source(getSession, ps).mapMaterializedValue(Future.successful)
      case Some(Failure(e)) =>
        Source.failed(e).mapMaterializedValue(_ => Future.failed(e))
      case None =>
        // completed later
        Source.fromFutureSource(prepStmt.map(ps => source(getSession, ps)))
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
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    currentEventsByTagInternal(tag, offset)
      .mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
      .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
  }

  private[akka] def currentEventsByTagInternal(tag: String, offset: Offset): Source[UUIDPersistentRepr, NotUsed] = {
    if (!config.eventsByTagEnabled)
      Source.failed(new IllegalStateException("Events by tag queries are disabled"))
    else {
      try {
        val (fromOffset, usingOffset) = offsetToInternalOffset(offset)
        val prereqs = eventsByTagPrereqs(tag, usingOffset, fromOffset)
        val toOffset = Some(offsetUuid(System.currentTimeMillis()))

        createFutureSource(prereqs) { (s, prereqs) =>
          val session = TagStageSession(tag, s, prereqs._1, queryPluginConfig.fetchSize)
          Source.fromGraph(EventsByTagStage(session, fromOffset, toOffset, queryPluginConfig, None, writePluginConfig.bucketSize, usingOffset, prereqs._2))
        }
          .via(deserializeEventsByTagRow)
          .mapMaterializedValue(_ => NotUsed)

      } catch {
        case NonFatal(e) =>
          // e.g. from cassandraSession, or selectStatement
          log.debug("Could not run currentEventsByTag [{}] query, due to: {}", tag, e.getMessage)
          Source.failed(e)
      }
    }
  }
  /**
   * `eventsByPersistenceId` is used to retrieve a stream of events for a particular persistenceId.
   *
   * In addition to the `offset` the `EventEnvelope` also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * `sequenceNr` and `offset` are always the same for an event and they define ordering for events
   * emitted by this query. Causality is guaranteed (`sequenceNr`s of events for a particular
   * `persistenceId` are always ordered in a sequence monotonically increasing by one). Multiple
   * executions of the same bounded stream are guaranteed to emit exactly the same stream of events.
   *
   * `fromSequenceNr` and `toSequenceNr` can be specified to limit the set of returned events.
   * The `fromSequenceNr` and `toSequenceNr` are inclusive.
   *
   * Deleted events are also deleted from the event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByPersistenceId`.
   */
  override def eventsByPersistenceId(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      queryPluginConfig.fetchSize,
      Some(queryPluginConfig.refreshInterval),
      s"eventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentRepr(eventsByPersistenceIdDeserializer, serialization))
      .mapMaterializedValue(_ => NotUsed)
      .map(p => mapEvent(p.persistentRepr))
      .mapConcat(r => toEventEnvelopes(r, r.sequenceNr))

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      queryPluginConfig.fetchSize,
      None,
      s"currentEventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentRepr(eventsByPersistenceIdDeserializer, serialization))
      .mapMaterializedValue(_ => NotUsed)
      .map(p => mapEvent(p.persistentRepr))
      .mapConcat(r => toEventEnvelopes(r, r.sequenceNr))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def eventsByPersistenceIdWithControl(
    persistenceId:   String,
    fromSequenceNr:  Long,
    toSequenceNr:    Long,
    refreshInterval: Option[FiniteDuration] = None): Source[EventEnvelope, Future[EventsByPersistenceIdStage.Control]] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      queryPluginConfig.fetchSize,
      refreshInterval.orElse(Some(queryPluginConfig.refreshInterval)),
      s"eventsByPersistenceId-$persistenceId",
      extractor = Extractors.persistentRepr(eventsByPersistenceIdDeserializer, serialization),
      fastForwardEnabled = true).map(p => mapEvent(p.persistentRepr))
      .mapConcat(r => toEventEnvelopes(r, r.sequenceNr))

  /**
   * INTERNAL API: This is a low-level method that return journal events as they are persisted.
   *
   * The fromJournal adaptation happens at higher level:
   *  - In the AsyncWriteJournal for the PersistentActor and PersistentView recovery.
   *  - In the public eventsByPersistenceId and currentEventsByPersistenceId queries.
   */
  @InternalApi private[akka] def eventsByPersistenceId[T](
    persistenceId:          String,
    fromSequenceNr:         Long,
    toSequenceNr:           Long,
    max:                    Long,
    fetchSize:              Int,
    refreshInterval:        Option[FiniteDuration],
    name:                   String,
    customConsistencyLevel: Option[ConsistencyLevel] = None,
    customRetryPolicy:      Option[RetryPolicy]      = None,
    extractor:              Extractor[T],
    fastForwardEnabled:     Boolean                  = false): Source[T, Future[EventsByPersistenceIdStage.Control]] = {

    val deserializeEventAsync = queryPluginConfig.deserializationParallelism > 1

    createFutureSource(combinedEventsByPersistenceIdStmts) { (s, c) =>
      log.debug("Creating EventByPersistentIdState graph")
      Source.fromGraph(new EventsByPersistenceIdStage(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        max,
        fetchSize,
        refreshInterval,
        EventsByPersistenceIdStage.EventsByPersistenceIdSession(
          c.preparedSelectEventsByPersistenceId,
          c.preparedSelectDeletedTo,
          s,
          customConsistencyLevel,
          customRetryPolicy),
        queryPluginConfig,
        fastForwardEnabled))
        .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
        .named(name)
    }
      .mapAsync(queryPluginConfig.deserializationParallelism) { row =>
        extractor.extract(row, deserializeEventAsync)
      }
      .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
  }

  /**
   * INTERNAL API: Internal hook for amending the event payload. Called from all queries.
   */
  @InternalApi private[akka] def mapEvent(persistentRepr: PersistentRepr): PersistentRepr =
    persistentRepr

  private[this] def toEventEnvelopes(persistentRepr: PersistentRepr, offset: Long): immutable.Iterable[EventEnvelope] =
    adaptFromJournal(persistentRepr).map { payload =>
      EventEnvelope(Offset.sequence(offset), persistentRepr.persistenceId, persistentRepr.sequenceNr, payload)
    }

  private[this] def toEventEnvelope(persistentRepr: PersistentRepr, offset: Offset): immutable.Iterable[EventEnvelope] =
    adaptFromJournal(persistentRepr).map { payload =>
      EventEnvelope(offset, persistentRepr.persistenceId, persistentRepr.sequenceNr, payload)
    }

  private[this] def internalUuidToOffset(uuid: UUID): Offset =
    if (uuid == firstOffset) NoOffset
    else TimeBasedUUID(uuid)

  private[this] def offsetToInternalOffset(offset: Offset): (UUID, Boolean) =
    offset match {
      case TimeBasedUUID(uuid) => (uuid, true)
      case NoOffset            => (firstOffset, false)
      case unsupported =>
        throw new IllegalArgumentException("Cassandra does not support " + unsupported.getClass.getName + " offsets")
    }

  private def adaptFromJournal(persistentRepr: PersistentRepr): immutable.Iterable[Any] = {
    val eventAdapter = eventAdapters.get(persistentRepr.payload.getClass)
    val eventSeq = eventAdapter.fromJournal(persistentRepr.payload, persistentRepr.manifest)
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
   *
   * Note the query is inefficient, especially for large numbers of `persistenceId`s, because
   * of limitation of current internal implementation providing no information supporting
   * ordering/offset queries. The query uses Cassandra's `select distinct` capabilities.
   * More importantly the live query has to repeatedly execute the query each `refresh-interval`,
   * because order is not defined and new `persistenceId`s may appear anywhere in the query results.
   */
  override def persistenceIds(): Source[String, NotUsed] =
    persistenceIds(Some(queryPluginConfig.refreshInterval), "allPersistenceIds")

  /**
   * Same type of query as `allPersistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] =
    persistenceIds(None, "currentPersistenceIds")

  private[this] def persistenceIds(
    refreshInterval: Option[FiniteDuration],
    name:            String): Source[String, NotUsed] =
    createSource[String, PreparedStatement](preparedSelectDistinctPersistenceIds, (s, ps) =>
      Source.actorPublisher[String](
        AllPersistenceIdsPublisher.props(
          refreshInterval,
          AllPersistenceIdsSession(ps, s),
          queryPluginConfig))
        .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
        .mapMaterializedValue(_ => NotUsed)
        .named(name))
}
