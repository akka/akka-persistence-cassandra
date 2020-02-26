/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID
import akka.annotation.ApiMayChange

/**
 * Events by tag query was unable all the events for some persistence ids.
 *
 * Consider restarting the query from the minOffset if downstream processing is idempotent
 * as it may re-deliver previously delivered events.
 *
 * @param tag the tag for the query
 * @param misssing a map from persistence id to a set of tag pid sequence numbers that could
 *                 not be found
 * @param minOffst minimum offset was used when searching
 * @param maxOffset maximum offset used when searching
 */
@ApiMayChange
final class MissingTaggedEventException(
    val tag: String,
    val missing: Map[String, Set[Long]],
    val minOffset: UUID,
    val maxOffset: UUID
) extends RuntimeException(
      s"Unable to find tagged events: ${missing}" +
      s"Tag: $tag")
