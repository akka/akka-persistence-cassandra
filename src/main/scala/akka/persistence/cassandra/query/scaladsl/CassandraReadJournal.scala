/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query.scaladsl

import java.net.URLEncoder
import java.util.UUID

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.{ Persistence, PersistentRepr }
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.query._
import akka.persistence.cassandra.query.AllPersistenceIdsPublisher.AllPersistenceIdsSession
import akka.persistence.cassandra.query.EventsByPersistenceIdPublisher.EventsByPersistenceIdSession
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.persistence.{ Persistence, PersistentRepr }
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ ConsistencyLevel, PreparedStatement, Session }
import com.typesafe.config.Config
import akka.persistence.cassandra.session.scaladsl.CassandraSession

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

  private case class CombinedEventsByPersistenceIdStmts(
    preparedSelectEventsByPersistenceId: PreparedStatement,
    preparedSelectInUse:                 PreparedStatement,
    preparedSelectDeletedTo:             PreparedStatement
  )
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
class CassandraReadJournal(system: ExtendedActorSystem, config: Config)
  extends ReadJournal
  with PersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  import CassandraReadJournal.CombinedEventsByPersistenceIdStmts

  private val log = Logging.getLogger(system, getClass)
  private val writePluginId = config.getString("write-plugin")
  private val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig(writePluginId))
  private val queryPluginConfig = new CassandraReadJournalConfig(config, writePluginConfig)
  private val eventAdapters = Persistence(system).adaptersFor(writePluginId)
  implicit private val ec = system.dispatchers.lookup(queryPluginConfig.pluginDispatcher)

  // TODO: change categoryUid to unique config path after akka/akka#19822 is fixed
  private val metricsCategory = {
    val categoryUid =
      CassandraReadJournal.InstanceUID.incrementAndGet() match {
        case 0     => ""
        case other => other
      }
    s"${CassandraReadJournal.Identifier}$categoryUid"
  }

  private val writeStatements: CassandraStatements = new CassandraStatements {
    def config: CassandraJournalConfig = writePluginConfig
  }
  private val queryStatements: CassandraReadStatements = new CassandraReadStatements {
    override def config = queryPluginConfig
  }

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
    init = session => writeStatements.executeCreateKeyspaceAndTables(
    session,
    writePluginConfig, writePluginConfig.maxTagId
  )
  )

  private def preparedSelectEventsByTag(tagId: Int): Future[PreparedStatement] = {
    require(tagId <= writePluginConfig.maxTagId)
    session.prepare(queryStatements.selectEventsByTag(tagId))
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true))
  }

  private def preparedSelectEventsByPersistenceId: Future[PreparedStatement] =
    session
      .prepare(writeStatements.selectMessages)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true))

  private def preparedSelectInUse: Future[PreparedStatement] =
    session
      .prepare(writeStatements.selectInUse)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true))

  private def preparedSelectDeletedTo: Future[PreparedStatement] =
    session
      .prepare(writeStatements.selectDeletedTo)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true))

  private def preparedSelectDistinctPersistenceIds: Future[PreparedStatement] =
    session
      .prepare(queryStatements.selectDistinctPersistenceIds)
      .map(_.setConsistencyLevel(queryPluginConfig.readConsistency).setIdempotent(true))

  private def combinedEventsByPersistenceIdStmts: Future[CombinedEventsByPersistenceIdStmts] =
    for {
      ps1 <- preparedSelectEventsByPersistenceId
      ps2 <- preparedSelectInUse
      ps3 <- preparedSelectDeletedTo
    } yield CombinedEventsByPersistenceIdStmts(ps1, ps2, ps3)

  system.registerOnTermination {
    session.close()
  }

  private def selectStatement(tag: String): Future[PreparedStatement] = {
    val tagId = writePluginConfig.tags.getOrElse(tag, 1)
    preparedSelectEventsByTag(tagId)
  }

  /**
   * Use this as the UUID offset in `eventsByTag` queries when you want all
   * events from the beginning of time.
   */
  val firstOffset: UUID = {
    // FIXME perhaps we can do something smarter, such as caching the highest offset retrieved
    // from queries
    val timestamp = queryPluginConfig.firstTimeBucket.startTimestamp
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
   * Max 3 tags per event is supported.
   *
   * You can use `NoOffset` to retrieve all events with a given tag or
   * retrieve a subset of all events by specifying a `TimeBasedUUID` `offset`.
   *
   * The offset of each event is provided in the streamed envelopes returned,
   * which makes it possible to resume the stream at a later point from a given offset.
   *
   * For querying events that happened after a long unix timestamp you can use [[#timeBasedUUIDFrom]]
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
   * executions of the query on a best effort basis. The query is using a Cassandra Materialized
   * View for the query and that is eventually consistent, so different queries may see different
   * events for the latest events, but eventually the result will be ordered by timestamp
   * (Cassandra timeuuid column). To compensate for the the eventual consistency the query is
   * delayed to not read the latest events, see `cassandra-query-journal.eventual-consistency-delay`
   * in reference.conf. However, this is only best effort and in case of network partitions
   * or other things that may delay the updates of the Materialized View the events may be
   * delivered in different order (not strictly by their timestamp).
   *
   * If you use the same tag for all events for a `persistenceId` it is possible to get
   * a more strict delivery order than otherwise. This can be useful when all events of
   * a PersistentActor class (all events of all instances of that PersistentActor class)
   * are tagged with the same tag. Then the events for each `persistenceId` can be delivered
   * strictly by sequence number. If a sequence number is missing the query is delayed up
   * to the configured `delayed-event-timeout` and if the expected event is still not
   * found the stream is completed with failure. This means that there must not be any
   * holes in the sequence numbers for a given tag, i.e. all events must be tagged
   * with the same tag. Set `delayed-event-timeout` to for example 30s to enable this
   * feature. It is disabled by default.
   *
   * Deleted events are also deleted from the tagged event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByTag`.
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    try {
      if (writePluginConfig.enableEventsByTagQuery) {
        require(tag != null && tag != "", "tag must not be null or empty")

        val fromOffset = offsetToInternalOffset(offset)

        import queryPluginConfig._
        createSource[EventEnvelope, PreparedStatement](selectStatement(tag), (s, ps) =>
          Source.actorPublisher[UUIDPersistentRepr](EventsByTagPublisher.props(tag, fromOffset,
            None, queryPluginConfig, s, ps))
            .mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
            .mapMaterializedValue(_ => NotUsed)
            .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
            .withAttributes(ActorAttributes.dispatcher(pluginDispatcher)))
      } else
        Source.failed(new UnsupportedOperationException("eventsByTag query is disabled"))
    } catch {
      case NonFatal(e) =>
        // e.g. from cassandraSession, or selectStatement
        log.debug("Could not run eventsByTag [{}] query, due to: {}", tag, e.getMessage)
        Source.failed(e)
    }
  }

  private def createSource[T, P](
    prepStmt: Future[P],
    source:   (Session, P) => Source[T, NotUsed]
  ): Source[T, NotUsed] = {
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

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   *
   * Use `NoOffset` when you want all events from the beginning of time.
   * To acquire an offset from a long unix timestamp to use with this query, you can use [[#timeBasedUUIDFrom]].
   *
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    try {
      if (writePluginConfig.enableEventsByTagQuery) {
        require(tag != null && tag != "", "tag must not be null or empty")
        import queryPluginConfig._

        val fromOffset = offsetToInternalOffset(offset)
        val toOffset = Some(offsetUuid(System.currentTimeMillis()))
        createSource[EventEnvelope, PreparedStatement](selectStatement(tag), (s, ps) =>
          Source.actorPublisher[UUIDPersistentRepr](EventsByTagPublisher.props(tag, fromOffset,
            toOffset, queryPluginConfig, s, ps))
            .mapConcat(r => toEventEnvelope(r.persistentRepr, TimeBasedUUID(r.offset)))
            .mapMaterializedValue(_ => NotUsed)
            .named("currentEventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
            .withAttributes(ActorAttributes.dispatcher(pluginDispatcher)))

      } else
        Source.failed(new UnsupportedOperationException("eventsByTag query is disabled"))
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
    toSequenceNr:   Long
  ): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      queryPluginConfig.fetchSize,
      Some(queryPluginConfig.refreshInterval),
      s"eventsByPersistenceId-$persistenceId"
    )
      .mapConcat(r => toEventEnvelopes(r, r.sequenceNr))

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long
  ): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Long.MaxValue,
      queryPluginConfig.fetchSize,
      None,
      s"currentEventsByPersistenceId-$persistenceId"
    )
      .mapConcat(r => toEventEnvelopes(r, r.sequenceNr))

  /**
   * This is a low-level method that return journal events as they are persisted.
   *
   * The fromJournal adaptation happens at higher level:
   *  - In the AsyncWriteJournal for the PersistentActor and PersistentView recovery.
   *  - In the public eventsByPersistenceId and currentEventsByPersistenceId queries.
   */
  private[cassandra] def eventsByPersistenceId(
    persistenceId:          String,
    fromSequenceNr:         Long,
    toSequenceNr:           Long,
    max:                    Long,
    fetchSize:              Int,
    refreshInterval:        Option[FiniteDuration],
    name:                   String,
    customConsistencyLevel: Option[ConsistencyLevel] = None
  ): Source[PersistentRepr, NotUsed] = {

    createSource[PersistentRepr, CombinedEventsByPersistenceIdStmts](combinedEventsByPersistenceIdStmts, (s, c) =>
      Source.actorPublisher[PersistentRepr](
        EventsByPersistenceIdPublisher.props(
          persistenceId,
          fromSequenceNr,
          toSequenceNr,
          max,
          fetchSize,
          refreshInterval,
          EventsByPersistenceIdSession(
            c.preparedSelectEventsByPersistenceId,
            c.preparedSelectInUse,
            c.preparedSelectDeletedTo,
            s,
            customConsistencyLevel
          ),
          queryPluginConfig
        )
      )
        .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
        .mapMaterializedValue(_ => NotUsed)
        .named(name))
  }

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

  private[this] def offsetToInternalOffset(offset: Offset): UUID =
    offset match {
      case TimeBasedUUID(uuid) => uuid
      case NoOffset            => firstOffset
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
    name:            String
  ): Source[String, NotUsed] =
    createSource[String, PreparedStatement](preparedSelectDistinctPersistenceIds, (s, ps) =>
      Source.actorPublisher[String](
        AllPersistenceIdsPublisher.props(
          refreshInterval,
          AllPersistenceIdsSession(ps, s),
          queryPluginConfig
        )
      )
        .withAttributes(ActorAttributes.dispatcher(queryPluginConfig.pluginDispatcher))
        .mapMaterializedValue(_ => NotUsed)
        .named(name))
}
