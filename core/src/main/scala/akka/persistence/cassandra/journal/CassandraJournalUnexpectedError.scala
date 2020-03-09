/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.CoordinatedShutdown.Reason

/**
 * Cassandra Journal unexpected error with akka.persistence.cassandra.coordinated-shutdown-on-error set to true
 */
case object CassandraJournalUnexpectedError extends Reason
