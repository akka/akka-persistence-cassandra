/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import org.scalatest.concurrent.Eventually
import akka.persistence.cassandra.TestTaggingActor

class TruncateAllSpec extends CassandraSpec with Eventually {

  val pid1 = "pid1"
  val pid2 = "pid2"
  val tag1 = "tag1"
  val tag2 = "tag2"
  val tag3 = "tag3"

  "Truncate " should {
    "remove all tags from the table" in {
      writeEventsFor(tag1, pid1, 2)
      writeEventsFor(Set(tag1, tag2, tag3), pid2, 2)

      expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2", "pid2 event-1", "pid2 event-2")
      expectEventsForTag(tag2, "pid2 event-1", "pid2 event-2")
      expectEventsForTag(tag3, "pid2 event-1", "pid2 event-2")

      val reconciliation = new Reconciliation(system)
      reconciliation.truncateTagView().futureValue

      eventually {
        expectEventsForTag(tag1)
        expectEventsForTag(tag2)
        expectEventsForTag(tag3)
      }
    }

    "recover if actors are started again" in {
      system.actorOf(TestTaggingActor.props(pid1))
      system.actorOf(TestTaggingActor.props(pid2))
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2", "pid2 event-1", "pid2 event-2")
        expectEventsForTag(tag2, "pid2 event-1", "pid2 event-2")
        expectEventsForTag(tag3, "pid2 event-1", "pid2 event-2")
      }

    }
  }

}
