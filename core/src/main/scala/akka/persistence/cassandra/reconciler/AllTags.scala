/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.annotation.InternalApi
import akka.stream.scaladsl.Source
import akka.NotUsed

/**
 * Calculates all the tags by scanning the tag_write_progress table.
 *
 * This is not an efficient query as it needs to do a full table scan and while running will keep
 * all tags in memory
 *
 * This may not pick up tags that have just been created as the write to the tag progress
 * table is asynchronous.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class AllTags(session: ReconciliationSession) {

  def execute(): Source[String, NotUsed] = {
    session
      .selectAllTagProgress()
      .map(_.getString("tag"))
      .statefulMapConcat(() => {
        var seen = Set.empty[String]
        tag =>
          if (!seen.contains(tag)) {
            seen += tag
            List(tag)
          } else {
            Nil
          }
      })
  }

}
