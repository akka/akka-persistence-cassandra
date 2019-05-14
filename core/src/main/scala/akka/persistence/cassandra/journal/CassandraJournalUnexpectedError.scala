package akka.persistence.cassandra.journal

import akka.actor.CoordinatedShutdown.Reason

/**
 * Cassandra Journal unexpected error with cassandra-journal.coordinated-shutdown-on-error set to true
 */
case object CassandraJournalUnexpectedError extends Reason
