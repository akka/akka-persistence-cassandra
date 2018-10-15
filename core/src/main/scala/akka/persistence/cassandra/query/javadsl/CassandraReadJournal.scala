/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query.javadsl

import java.util.UUID
import java.util.concurrent.CompletionStage

import akka.{ Done, NotUsed }
import akka.persistence.cassandra.session.javadsl.CassandraSession
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.TimeBasedUUID
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source

import scala.compat.java8.FutureConverters

object CassandraReadJournal {
  /**
   * The default identifier for [[CassandraReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#getReadJournalFor`.
   *
   * The value is `"cassandra-query-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.Identifier
}

/**
 * Java API: `akka.persistence.query.javadsl.ReadJournal` implementation for Cassandra.
 *
 * It is retrieved with:
 * {{{
 * CassandraReadJournal queries =
 *   PersistenceQuery.get(system).getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[akka.persistence.cassandra.query.scaladsl.CassandraReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"cassandra-query-journal"`
 * for the default [[CassandraReadJournal#Identifier]]. See `reference.conf`.
 *
 */
class CassandraReadJournal(scaladslReadJournal: akka.persistence.cassandra.query.scaladsl.CassandraReadJournal)
  extends ReadJournal
  with PersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  /**
   * Data Access Object for arbitrary queries or updates.
   */
  def session: CassandraSession = new CassandraSession(scaladslReadJournal.session)

  /**
   * Initialize connection to Cassandra and prepared statements.
   * It is not required to do this and it will happen lazily otherwise.
   * It is also not required to wait until this CompletionStage is complete to start
   * using the read journal.
   */
  def initialize(): CompletionStage[Done] = FutureConverters.toJava(scaladslReadJournal.initialize())

  /**
   * Use this as the UUID offset in `eventsByTag` queries when you want all
   * events from the beginning of time.
   */
  def firstOffset: UUID = scaladslReadJournal.firstOffset

  /**
   * Create a time based UUID that can be used as offset in [[eventsByTag]]
   * queries. The timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def offsetUuid(timestamp: Long): UUID = scaladslReadJournal.offsetUuid(timestamp)

  /**
   * Create a time based UUID that can be used as offset in [[eventsByTag]]
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def timeBasedUUIDFrom(timestamp: Long): Offset = scaladslReadJournal.timeBasedUUIDFrom(timestamp)

  /**
   * Convert a `TimeBasedUUID` to a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def timestampFrom(offset: TimeBasedUUID): Long = scaladslReadJournal.timestampFrom(offset)

  /**
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an `akka.persistence.journal.EventAdapter` that wraps the events
   * in a `akka.persistence.journal.Tagged` with the given `tags`.
   * The tags must be defined in the `tags` section of the `cassandra-journal` configuration.
   *
   * You can use [[akka.persistence.query.NoOffset]] to retrieve all events with a given tag or
   * retrieve a subset of all events by specifying a [[TimeBasedUUID]] `offset`.
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
   * separate table that so is eventually consistent.
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
    scaladslReadJournal.eventsByTag(tag, offset).asJava

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
    scaladslReadJournal.currentEventsByTag(tag, offset).asJava

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
    scaladslReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal
      .currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .asJava

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
  override def persistenceIds(): Source[String, NotUsed] = scaladslReadJournal.persistenceIds().asJava

  /**
   * Same type of query as `allPersistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] =
    scaladslReadJournal.currentPersistenceIds().asJava
}
