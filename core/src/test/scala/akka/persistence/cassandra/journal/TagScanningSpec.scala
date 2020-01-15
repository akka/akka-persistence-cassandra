/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, TestTaggingActor }
import com.typesafe.config.ConfigFactory

object TagScanningSpec {
  val config = ConfigFactory.parseString(s"""
      cassandra-journal.events-by-tag.enabled = on
      cassandra-journal.events-by-tag.scanning-flush-interval = 2s
      cassandra-journal.write.replay-filter.mode = off
      cassandra-journal.log-queries = off
    """).withFallback(CassandraLifecycle.config)
}

class TagScanningSpec extends CassandraSpec(TagScanningSpec.config) {

  "Tag writing" must {
    "complete writes to tag scanning for many persistent actors" in {
      val nrActors = 25
      (0 until nrActors).foreach { i =>
        val ref = system.actorOf(TestTaggingActor.props(s"$i"))
        ref ! "msg"
      }

      awaitAssert {
        import scala.collection.JavaConverters._
        val expected = (0 until nrActors).map(n => (s"$n".toInt, 1L)).toList
        val scanning = cluster
          .execute(s"select * from ${journalName}.tag_scanning")
          .all()
          .asScala
          .toList
          .map(row => (row.getString("persistence_id"), row.getLong("sequence_nr")))
          .filterNot(_._1.startsWith("persistenceInit"))
          .map { case (pid, seqNr) => (pid.toInt, seqNr) } // sorting by pid makes the failure message easy to interpret
          .sortBy(_._1)
        scanning shouldEqual expected
      }
    }
  }
}
