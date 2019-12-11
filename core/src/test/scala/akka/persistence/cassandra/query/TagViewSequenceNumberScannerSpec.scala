/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID

import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal.PersistenceId
import akka.persistence.cassandra.journal.CassandraJournal.TagPidSequenceNr
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.Hour
import akka.persistence.cassandra.query.TagViewSequenceNumberScannerSpec.config
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.typesafe.config.ConfigFactory

object TagViewSequenceNumberScannerSpec {
  val bucketSize = Hour
  val name = "EventsByTagSequenceNumberScanningSpec"
  val config = ConfigFactory.parseString(s"""
      |cassandra-journal.events-by-tag.bucket-size = ${bucketSize.toString}
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class TagViewSequenceNumberScannerSpec extends CassandraSpec(config) with TestTagWriter {

  import TagViewSequenceNumberScannerSpec._

  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  val serialization: Serialization = SerializationExtension(system)

  "Tag Pid Sequence Number Scanning" must {
    "be empty for no events" in {
      val now = Uuids.timeBased()
      val pidSequenceNrs = queries.scanTagSequenceNrs("Tag1", now).futureValue
      pidSequenceNrs should equal(Map.empty[PersistenceId, (TagPidSequenceNr, UUID)])
    }

    "pick the lowest sequence number after the offset and deduct 1" in {
      // not picked up as before the offset
      writeTaggedEvent(PersistentRepr("p2e4", persistenceId = "p2"), Set("blue"), 4, bucketSize)
      val now = Uuids.timeBased()
      writeTaggedEvent(PersistentRepr("p1e1", persistenceId = "p1"), Set("blue"), 1, bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", persistenceId = "p1"), Set("blue"), 2, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e1", persistenceId = "p2"), Set("blue"), 5, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e2", persistenceId = "p2"), Set("blue"), 6, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e3", persistenceId = "p2"), Set("blue"), 7, bucketSize)
      val pidSequenceNrs = queries.scanTagSequenceNrs("blue", now).futureValue.map {
        case (persistenceId, (tagSeqNr, _)) => (persistenceId, tagSeqNr)
      }
      pidSequenceNrs should equal(Map("p1" -> 0, "p2" -> 4))
    }
  }
}
