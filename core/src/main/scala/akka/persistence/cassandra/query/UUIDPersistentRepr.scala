/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID

import akka.persistence.PersistentRepr
import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.CassandraJournal.TagPidSequenceNr

/**
 * INTERNAL API: Wrap the [[PersistentRepr]] to add the UUID for
 * `eventsByTag` query, or similar queries.
 */
@InternalApi private[akka] final case class UUIDPersistentRepr(
  offset: UUID,
  //  tags:             Set[String], this isn't in the tags table, may need to be added
  tagPidSequenceNr: TagPidSequenceNr,
  persistentRepr:   PersistentRepr)
