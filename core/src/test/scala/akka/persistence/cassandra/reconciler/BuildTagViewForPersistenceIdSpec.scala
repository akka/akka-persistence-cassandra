/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import org.scalatest.concurrent.Eventually

class BuildTagViewForPersistenceIdSpec extends CassandraSpec with Eventually {

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
      val reconciliation = new Reconciliation(system)
      reconciliation.truncateTagView().futureValue
      expectEventsForTag(tag1)
      reconciliation.rebuildTagViewForPersistenceIds(pid1).futureValue
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2")
      }
      reconciliation.rebuildTagViewForPersistenceIds(pid2).futureValue
      eventually {
        expectEventsForTag(tag1, "pid1 event-1", "pid1 event-2", "pid2 event-1")
      }
    }
  }

  "continueTagWritesForPersistenceId" should {

    "be idempotent when run multiple times on up-to-date data" in {
      val tag3 = "tag3"
      val pid4 = "pid4"

      writeEventsFor(tag3, pid4, 3)
      eventually {
        expectEventsForTag(tag3, "pid4 event-1", "pid4 event-2", "pid4 event-3")
      }

      val reconciliation = new Reconciliation(system)

      // Running continue when everything is already up to date should be safe
      // It reads progress, finds no events > progress, does nothing
      reconciliation.continueTagWritesForPersistenceId(pid4).futureValue
      reconciliation.continueTagWritesForPersistenceId(pid4).futureValue

      // Events should still be there, no duplicates
      eventually {
        expectEventsForTag(tag3, "pid4 event-1", "pid4 event-2", "pid4 event-3")
      }
    }

    "rebuild all events when no progress exists" in {
      val tag2 = "tag2"
      val pid3 = "pid3"

      // Write events and let them be tagged normally
      writeEventsFor(tag2, pid3, 3)
      eventually {
        expectEventsForTag(tag2, "pid3 event-1", "pid3 event-2", "pid3 event-3")
      }

      val reconciliation = new Reconciliation(system)

      // deleteTagViewForPersistenceIds deletes BOTH tag_views AND progress
      reconciliation.deleteTagViewForPersistenceIds(Set(pid3), tag2).futureValue
      expectEventsForTag(tag2)

      // With no progress, continue reads from seqNr 0 and writes all events
      reconciliation.continueTagWritesForPersistenceId(pid3).futureValue
      eventually {
        expectEventsForTag(tag2, "pid3 event-1", "pid3 event-2", "pid3 event-3")
      }
    }
  }

  "rebuildTagViewForPersistenceIds" should {

    "restore all events after deletion" in {
      val tag5 = "tag5"
      val pid6 = "pid6"

      writeEventsFor(tag5, pid6, 5)
      eventually {
        expectEventsForTag(tag5, "pid6 event-1", "pid6 event-2", "pid6 event-3", "pid6 event-4", "pid6 event-5")
      }

      val reconciliation = new Reconciliation(system)

      // Delete tag views (this also deletes progress)
      reconciliation.deleteTagViewForPersistenceIds(Set(pid6), tag5).futureValue
      expectEventsForTag(tag5)

      // Rebuild should restore ALL events
      reconciliation.rebuildTagViewForPersistenceIds(pid6).futureValue
      eventually {
        expectEventsForTag(tag5, "pid6 event-1", "pid6 event-2", "pid6 event-3", "pid6 event-4", "pid6 event-5")
      }
    }

    "be idempotent when run multiple times after deletion" in {
      val tag4 = "tag4"
      val pid5 = "pid5"

      writeEventsFor(tag4, pid5, 3)
      eventually {
        expectEventsForTag(tag4, "pid5 event-1", "pid5 event-2", "pid5 event-3")
      }

      val reconciliation = new Reconciliation(system)

      // Delete first to start clean
      reconciliation.deleteTagViewForPersistenceIds(Set(pid5), tag4).futureValue
      expectEventsForTag(tag4)

      // Running rebuild multiple times should be idempotent
      // (since we're starting from empty tag_views each time after delete)
      reconciliation.rebuildTagViewForPersistenceIds(pid5).futureValue
      eventually {
        expectEventsForTag(tag4, "pid5 event-1", "pid5 event-2", "pid5 event-3")
      }

      // Delete again
      reconciliation.deleteTagViewForPersistenceIds(Set(pid5), tag4).futureValue
      expectEventsForTag(tag4)

      // Rebuild again - should produce same result
      reconciliation.rebuildTagViewForPersistenceIds(pid5).futureValue
      eventually {
        expectEventsForTag(tag4, "pid5 event-1", "pid5 event-2", "pid5 event-3")
      }
    }
  }
}
