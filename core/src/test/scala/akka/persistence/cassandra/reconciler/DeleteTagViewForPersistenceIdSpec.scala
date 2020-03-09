/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.TestTaggingActor
import akka.testkit.TestProbe
import akka.persistence.RecoveryCompleted

/**
 * These tests depend on the output of each other, can't be run separately
 */
class DeleteTagViewForPersistenceIdSpec extends CassandraSpec {

  "Deleting " should {
    val tag = "tag1"
    val pid1 = "p1"
    val pid2 = "p2"

    "only delete for the provided persistence id" in {
      writeEventsFor(tag, pid1, 3)
      writeEventsFor(tag, pid2, 3)

      eventsByTag(tag)
        .request(10)
        .expectNextN(List("p1 event-1", "p1 event-2", "p1 event-3", "p2 event-1", "p2 event-2", "p2 event-3"))
        .expectNoMessage()
        .cancel()
      val reconciliation = new Reconciliation(system)
      reconciliation.deleteTagViewForPersistenceIds(Set(pid2), tag).futureValue
      eventsByTag(tag).request(5).expectNextN(List("p1 event-1", "p1 event-2", "p1 event-3")).expectNoMessage().cancel()

    }

    "recover the tagged events if persistence id is started again" in {
      val probe = TestProbe()
      system.actorOf(TestTaggingActor.props(pid2, Set(tag), Some(probe.ref)))
      probe.expectMsg(RecoveryCompleted)
      eventsByTag(tag)
        .request(10)
        .expectNextN(List("p1 event-1", "p1 event-2", "p1 event-3", "p2 event-1", "p2 event-2", "p2 event-3"))
        .expectNoMessage()
        .cancel()
    }

  }

}
