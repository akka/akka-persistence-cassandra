/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.Eventually

class RebuildAllPersisetceIdsSpec extends CassandraSpec with Eventually {

  "RebuildAllPersisetceIds" should {

    val tag1 = "tag1"
    val pid1 = "pid1"
    val pid2 = "pid2"
    val pid3 = "pid3"
    val pid4 = "pid4"
    val pid5 = "pid5"

    "build from messages table" in {
      writeEventsFor(tag1, pid1, 2)
      writeEventsFor(tag1, pid2, 1)
      writeEventsFor(tag1, pid3, 5)

      val reconciliation = new Reconciliation(system)
      reconciliation.rebuildAllPersistenceIds().futureValue

      queries
        .currentPersistenceIds()
        .runWith(Sink.seq)
        .futureValue
        .toSet
        .filterNot(_.startsWith("persistenceInit")) should ===(Set(pid1, pid2, pid3))

      // add some more
      writeEventsFor(tag1, pid4, 2)
      writeEventsFor(tag1, pid5, 4)

      reconciliation.rebuildAllPersistenceIds().futureValue

      queries
        .currentPersistenceIds()
        .runWith(Sink.seq)
        .futureValue
        .toSet
        .filterNot(_.startsWith("persistenceInit")) should ===(Set(pid1, pid2, pid3, pid4, pid5))
    }
  }
}
