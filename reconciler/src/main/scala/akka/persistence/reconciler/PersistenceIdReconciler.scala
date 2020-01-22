/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.query.PersistenceQuery
import akka.actor.ClassicActorSystemProvider
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.query._
import akka.persistence.cassandra.journal._
import akka.serialization.SerializationExtension

/**
 * Walks through a persistence id and checks the tag_view table has all of the events in.
 *
 * Can existing evnets by persistnce query be used?
 *
 * Can existing evnets by tag query be used?
 *
 * Starts at the beginning of an event log for a persistence id.
 *
 * On the first tagged event starts an evnets by tag query at that offset to check
 * it exists.
 *
 * Validates that the events by tag for a persistence id has consequative sequence numbers
 * with no duplicates.
 *
 * Fix modes:
 *  - Delete peristence id in tag_views table. Done via issuing full partition deletes
 *    from a given start timebucket to the current time bucket
 *  - Re-write a persistence id into tag_views
 *  - Not supported yet: any kinf of incremental repair
 */
object PersistenceIdReconciler {}

class PersistenceIdReconciler(provider: ClassicActorSystemProvider, persistenceId: String) {

  val readJournal =
    PersistenceQuery(provider.classicSystem).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val eventDeserialzier = new CassandraJournal.EventDeserializer(provider.classicSystem)

  val serialization = SerializationExtension(provider.classicSystem)

  val taggedEvents = readJournal.eventsByPersistenceId(
    persistenceId,
    0,
    Long.MaxValue,
    Long.MaxValue,
    None,
    "cassandra-plugin",
    s"reconciler-$persistenceId",
    EventsByPersistenceIdStage.Extractors.taggedPersistentRepr(eventDeserialzier, serialization))

}
