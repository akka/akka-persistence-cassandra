package akka.persistence.cassandra.query.scaladsl

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import java.net.URLEncoder
import java.util.UUID

import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.cassandra.query.AllPersistenceIdsPublisher.AllPersistenceIdsSession
import akka.persistence.cassandra.query.EventsByPersistenceIdPublisher.EventsByPersistenceIdSession
import akka.persistence.cassandra.query._
import akka.persistence.cassandra.retry
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.Config

import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements

object CassandraReadJournal {
  /**
   * The default identifier for [[CassandraReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"cassandra-query-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = "cassandra-query-journal"
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
  with AllPersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  private val log = Logging.getLogger(system, getClass)
  private val writePluginConfig = new CassandraJournalConfig(system.settings.config.getConfig(config.getString("write-plugin")))
  private val queryPluginConfig = new CassandraReadJournalConfig(config, writePluginConfig)

  private class CassandraSession {

    val underlying: Session = connect()

    private def connect(): Session = {
      val cluster = writePluginConfig.clusterBuilder.build
      retry(writePluginConfig.connectionRetries + 1, writePluginConfig.connectionRetryDelay.toMillis) {
        cluster.connect()
      }
    }

    private val writeStatements: CassandraStatements = new CassandraStatements {
      def config: CassandraJournalConfig = writePluginConfig
    }
    private val queryStatements: CassandraReadStatements = new CassandraReadStatements {
      override def config = queryPluginConfig
    }
    if (writePluginConfig.keyspaceAutoCreate) {
      akka.persistence.cassandra.retry(writePluginConfig.keyspaceAutoCreateRetries) {
        underlying.execute(writeStatements.createKeyspace)
      }
    }

    underlying.execute(writeStatements.createTable)

    for (tagId <- 1 to writePluginConfig.maxTagId)
      underlying.execute(writeStatements.createEventsByTagMaterializedView(tagId))

    val preparedSelectEventsByTag: Vector[PreparedStatement] =
      (1 to writePluginConfig.maxTagId).map { tagId =>
        underlying.prepare(queryStatements.selectEventsByTag(tagId))
          .setConsistencyLevel(queryPluginConfig.readConsistency)
      }.toVector

    val preparedSelectEventsByPersistenceId: PreparedStatement =
      session
        .prepare(writeStatements.selectMessages)
        .setConsistencyLevel(queryPluginConfig.readConsistency)

    val preparedSelectInUse: PreparedStatement =
      session
        .prepare(writeStatements.selectInUse)
        .setConsistencyLevel(queryPluginConfig.readConsistency)

    val preparedSelectDeletedTo =
      session
        .prepare(writeStatements.selectDeletedTo)
        .setConsistencyLevel(queryPluginConfig.readConsistency)

    val preparedSelectDistinctPersistenceIds =
      session
        .prepare(queryStatements.selectDistinctPersistenceIds)
        .setConsistencyLevel(queryPluginConfig.readConsistency)
  }

  @volatile private var sessionUsed = false

  private lazy val cassandraSession: CassandraSession = {
    val s = new CassandraSession
    sessionUsed = true
    s
  }

  system.registerOnTermination {
    if (sessionUsed) {
      cassandraSession.underlying.close()
      cassandraSession.underlying.getCluster().close()
    }
  }

  private def selectStatement(tag: String): PreparedStatement = {
    val tagId = writePluginConfig.tags.getOrElse(tag, 1)
    cassandraSession.preparedSelectEventsByTag(tagId - 1)
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
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an `akka.persistence.journal.EventAdapter` that wraps the events
   * in a `akka.persistence.journal.Tagged` with the given `tags`.
   * The tags must be defined in the `tags` section of the `cassandra-journal` configuration.
   * Max 3 tags per event is supported.
   *
   * You can retrieve a subset of all events by specifying `offset`, or use `0L` to retrieve all
   * events with a given tag. The `offset` corresponds to a timesamp of the events. Note that the
   * corresponding offset of each event is provided in the `akka.persistence.query.EventEnvelope`,
   * which makes it possible to resume the stream at a later point from a given offset.
   * The `offset` is inclusive, i.e. the events with the exact same timestamp will be included
   * in the returned stream.
   *
   * There is a variant of this query with a time based UUID as offset. That query is better
   * for cases where you want to resume the stream from an exact point without receiving
   * duplicate events for the same timestamp.
   *
   * In addition to the `offset` the `EventEnvelope` also provides `persistenceId` and `sequenceNr`
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
   * For each `persistenceId` the events are delivered strictly by sequence number. If
   * a sequence number is missing the query is delayed up to the configured
   * `delayed-event-timeout` and if the expected event is still not found
   * the stream is completed with failure.
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
  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    eventsByTag(tag, offsetUuid(offset))
      .map(env => EventEnvelope(
        offset = UUIDs.unixTimestamp(env.offset),
        persistenceId = env.persistenceId,
        sequenceNr = env.sequenceNr,
        event = env.event))
  }

  /**
   * Same type of query as the `eventsByTag` query with `Long` offset, but this one has
   * a time based UUID `offset`. This query is better for cases where you want to resume
   * the stream from an exact point without receiving duplicate events for the same
   * timestamp.
   *
   * Use [[#firstOffset]] when you want all events from the beginning of time.
   *
   * The `offset` is exclusive, i.e. the event with the exact same UUID will not be included
   * in the returned stream.
   */
  def eventsByTag(tag: String, offset: UUID): Source[UUIDEventEnvelope, Unit] =
    try {
      import queryPluginConfig._
      Source.actorPublisher[UUIDEventEnvelope](EventsByTagPublisher.props(tag, offset,
        None, queryPluginConfig, cassandraSession.underlying, selectStatement(tag)))
        .mapMaterializedValue(_ => ())
        .named("eventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
        .withAttributes(ActorAttributes.dispatcher(pluginDispatcher))
    } catch {
      case NonFatal(e) =>
        // e.g. from cassandraSession, or selectStatement
        log.debug("Could not run eventsByTag [{}] query, due to: {}", tag, e.getMessage)
        Source.failed(e)
    }

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   *
   * The `offset` is inclusive, i.e. the events with the exact same timestamp will be included
   * in the returned stream.
   */
  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    currentEventsByTag(tag, offsetUuid(offset))
      .map(env => EventEnvelope(
        offset = UUIDs.unixTimestamp(env.offset),
        persistenceId = env.persistenceId,
        sequenceNr = env.sequenceNr,
        event = env.event))
  }

  /**
   * Same type of query as the `currentEventsByTag` query with `Long` offset, but this one has
   * a time based UUID `offset`. This query is better for cases where you want to resume
   * the stream from an exact point without receiving duplicate events for the same
   * timestamp.
   *
   * Use [[#firstOffset]] when you want all events from the beginning of time.
   *
   * The `offset` is exclusive, i.e. the event with the exact same UUID will not be included
   * in the returned stream.
   */
  def currentEventsByTag(tag: String, offset: UUID): Source[UUIDEventEnvelope, Unit] =
    try {
      import queryPluginConfig._
      val toOffset = Some(offsetUuid(System.currentTimeMillis()))
      Source.actorPublisher[UUIDEventEnvelope](EventsByTagPublisher.props(tag, offset,
        toOffset, queryPluginConfig, cassandraSession.underlying, selectStatement(tag)))
        .mapMaterializedValue(_ => ())
        .named("currentEventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
        .withAttributes(ActorAttributes.dispatcher(pluginDispatcher))
    } catch {
      case NonFatal(e) =>
        // e.g. from cassandraSession, or selectStatement
        log.debug("Could not run currentEventsByTag [{}] query, due to: {}", tag, e.getMessage)
        Source.failed(e)
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
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Some(queryPluginConfig.refreshInterval),
      s"eventsByPersistenceId-$persistenceId")

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    eventsByPersistenceId(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      None,
      s"currentEventsByPersistenceId-$persistenceId")

  private[this] def eventsByPersistenceId(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    refreshInterval: Option[FiniteDuration],
    name: String) = {

    Source.actorPublisher[EventEnvelope](
      EventsByPersistenceIdPublisher.props(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        refreshInterval,
        EventsByPersistenceIdSession(
          cassandraSession.preparedSelectEventsByPersistenceId,
          cassandraSession.preparedSelectInUse,
          cassandraSession.preparedSelectDeletedTo,
          cassandraSession.session),
        queryPluginConfig))
      .mapMaterializedValue(_ => ())
      .named(name)
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
  def allPersistenceIds(): Source[String, Unit] =
    persistenceIds(Some(queryPluginConfig.refreshInterval), "allPersistenceIds")

  /**
   * Same type of query as `allPersistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  def currentPersistenceIds(): Source[String, Unit] =
    persistenceIds(None, "currentPersistenceIds")

  private[this] def persistenceIds(
      refreshInterval: Option[FiniteDuration],
      name: String): Source[String, Unit] =
    Source.actorPublisher[String](
      AllPersistenceIdsPublisher.props(
        refreshInterval,
        AllPersistenceIdsSession(
          cassandraSession.preparedSelectDistinctPersistenceIds,
          cassandraSession.session),
        queryPluginConfig))
      .mapMaterializedValue(_ => ())
      .named(name)
}
