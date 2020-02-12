/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.TestTaggingActor
import akka.persistence.cassandra.TestTaggingActor.Ack

class DeleteTagViewForPersistenceIdSpec extends CassandraSpec("""

""") {

  // FIXME use nextId once https://github.com/akka/akka-persistence-cassandra/pull/681/is merged
  //

  "Deleting " should {

    "work" in {
      val tag = "tag1"
      val pid = "p1"
      val ref = system.actorOf(TestTaggingActor.props(pid, Set(tag)))

      for (i <- 1 to 3) {
        ref ! s"event-$i"
        expectMsg(Ack)
      }

      eventsByTag(tag).request(5).expectNextN(List("event-1", "event-2", "event-3")).expectNoMessage().cancel()

      Reconciliation(system).deleteTagViewForPersistenceId(pid).futureValue

      eventsByTag(tag).request(5).expectNextN(Nil).expectNoMessage().cancel()
    }
  }

}
