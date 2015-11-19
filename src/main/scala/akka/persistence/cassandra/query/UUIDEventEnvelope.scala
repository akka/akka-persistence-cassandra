package akka.persistence.cassandra.query

import java.util.UUID

/**
 * Event wrapper adding meta data for the events in the result stream of
 * `eventsByTag` query, or similar queries.
 */
final case class UUIDEventEnvelope(
  offset: UUID,
  persistenceId: String,
  sequenceNr: Long,
  event: Any)
