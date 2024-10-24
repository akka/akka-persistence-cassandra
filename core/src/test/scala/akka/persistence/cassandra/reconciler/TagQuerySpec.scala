/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.persistence.cassandra.CassandraSpec
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.Eventually

class TagQuerySpec extends CassandraSpec with Eventually {

  private lazy val reconciliation = new Reconciliation(system)

  "Tag querying" should {
    "return distinct tags for all tags" in {
      val pid1 = "pid1"
      val pid2 = "pid2"
      val tag1 = "tag1"
      val tag2 = "tag2"
      val tag3 = "tag3"
      reconciliation.allTags().runWith(Sink.seq).futureValue shouldEqual Nil
      writeEventsFor(Set(tag1, tag2), pid1, 3)
      writeEventsFor(Set(tag2, tag3), pid2, 3)
      eventually {
        val allTags = reconciliation.allTags().runWith(Sink.seq).futureValue
        allTags.size shouldEqual 3
        allTags.toSet shouldEqual Set(tag1, tag2, tag3)
      }
    }

    "return tags only if that pid has used them" in {
      val pid1 = "p11"
      val pid2 = "p12"
      val tag1 = "tag11"
      val tag2 = "tag12"
      val tag3 = "tag13"
      writeEventsFor(tag1, pid1, 3)
      writeEventsFor(Set(tag2, tag3), pid2, 3)
      eventually {
        val tags = reconciliation.tagsForPersistenceId(pid2).futureValue
        tags.size shouldEqual 2
        tags.toSet shouldEqual Set(tag2, tag3)
      }
    }
  }

}
