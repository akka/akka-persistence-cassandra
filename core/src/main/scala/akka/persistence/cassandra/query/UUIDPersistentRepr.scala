/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.util.UUID

import akka.persistence.PersistentRepr
import akka.annotation.InternalApi

/**
 * INTERNAL API: Wrap the [[PersistentRepr]] to add the UUID for
 * `eventsByTag` query, or similar queries.
 */
@InternalApi private[akka] final case class UUIDPersistentRepr(
  offset:         UUID,
  persistentRepr: PersistentRepr
)
