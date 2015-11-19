package akka.persistence.cassandra.query.javadsl

import scala.concurrent.duration._
import akka.persistence.query.EventEnvelope
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source
import akka.persistence.cassandra.query.UUIDEventEnvelope
import java.util.UUID

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
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  /**
   * Use this as the UUID offset in `eventsByTag` queries when you want all
   * events from the beginning of time.
   */
  def firstOffset: UUID = scaladslReadJournal.firstOffset

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   */
  def offsetUuid(timestamp: Long): UUID = scaladslReadJournal.offsetUuid(timestamp)

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
  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.eventsByTag(tag, offset).asJava

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
    scaladslReadJournal.eventsByTag(tag, offset).asJava

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   *
   * The `offset` is inclusive, i.e. the events with the exact same timestamp will be included
   * in the returned stream.
   */
  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.currentEventsByTag(tag, offset).asJava

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
    scaladslReadJournal.currentEventsByTag(tag, offset).asJava
}

