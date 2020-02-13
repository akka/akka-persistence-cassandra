/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import org.scalatest.concurrent.Eventually

class BuildTagViewForPersisetceIdSpec extends CassandraSpec with Eventually {

  "BuildTagViewForPersistenceId" should {

    val tag1 = "tag1"
    val pid1 = "pid1"
    val pid2 = "pid2"

    "build from scratch" in {
      writeEventsFor(tag1, pid1, 2)
      writeEventsFor(tag1, pid2, 1)
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2", "pid2 event-1")
      }
      Reconciliation(system).truncateTagView().futureValue
      expectEventsForTag(tag1)
      Reconciliation(system).rebuildTagViewForPersistenceIds(pid1).futureValue
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2")
      }
      Reconciliation(system).rebuildTagViewForPersistenceIds(pid2).futureValue
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2", "pid2 event-1")
      }
    }
  }
}
