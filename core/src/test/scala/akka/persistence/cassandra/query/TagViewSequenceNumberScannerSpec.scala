/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }
import java.util.UUID

import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal.{ PersistenceId, TagPidSequenceNr }
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Hour }
import akka.persistence.cassandra.query.TagViewSequenceNumberScannerSpec.config
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.serialization.{ Serialization, SerializationExtension }
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.time.{ Seconds, Span }

import scala.concurrent.duration.Duration

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object TagViewSequenceNumberScannerSpec {
  val bucketSize = Hour
  val name = "EventsByTagSequenceNumberScanningSpec"
  val config = ConfigFactory.parseString(s"""
      |akka.loglevel = INFO
      |cassandra-journal.events-by-tag.bucket-size = ${bucketSize.toString}
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class TagViewSequenceNumberScannerSpec extends CassandraSpec(config) with TestTagWriter with BeforeAndAfter {

  import TagViewSequenceNumberScannerSpec._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  val serialization: Serialization = SerializationExtension(system)
  lazy val session: Session = {
    import system.dispatcher
    Await.result(writePluginConfig.sessionProvider.connect(), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    Try {
      session.close()
      session.getCluster.close()
    }
    super.afterAll()
  }

  before {
    clearAllEvents()
  }

  "Tag Pid Sequence Number Scanning" must {

    "be empty for no events" in {
      val now = UUIDs.timeBased()
      val pidSequenceNrs = queries.calculateStartingTagPidSequenceNrs("Tag1", now).futureValue
      pidSequenceNrs should equal(Map.empty[PersistenceId, (TagPidSequenceNr, UUID)])
    }

    "pick the lowest sequence number after the offset and deduct 1" in {
      // not picked up as before the offset
      writeTaggedEvent(PersistentRepr("p2e4", persistenceId = "p2"), Set("blue"), 4, bucketSize)
      val now = UUIDs.timeBased()
      writeTaggedEvent(PersistentRepr("p1e1", persistenceId = "p1"), Set("blue"), 1, bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", persistenceId = "p1"), Set("blue"), 2, bucketSize)

      writeTaggedEvent(PersistentRepr("p2e1", persistenceId = "p2"), Set("blue"), 5, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e2", persistenceId = "p2"), Set("blue"), 6, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e3", persistenceId = "p2"), Set("blue"), 7, bucketSize)

      val pidSequenceNrs = queries.calculateStartingTagPidSequenceNrs("blue", now).futureValue.map {
        case (persistenceId, (tagSeqNr, _)) => (persistenceId, tagSeqNr)
      }
      pidSequenceNrs should equal(Map("p1" -> 0, "p2" -> 4))
    }

    "allow selecting of max pid sequence nrs" in {
      // not picked up as before the offset
      val before = UUIDs.timeBased()
      writeTaggedEvent(PersistentRepr("p1e1", persistenceId = "p1"), Set("blue"), 1, bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", persistenceId = "p1"), Set("blue"), 2, bucketSize)

      writeTaggedEvent(PersistentRepr("p2e1", persistenceId = "p2"), Set("blue"), 5, bucketSize)
      writeTaggedEvent(PersistentRepr("p2e2", persistenceId = "p2"), Set("blue"), 6, bucketSize)
      val after = UUIDs.timeBased()
      // not picked up as is after
      writeTaggedEvent(PersistentRepr("p2e3", persistenceId = "p2"), Set("blue"), 7, bucketSize)

      val pidSequenceNrs = queries.tagViewScanner.futureValue
        .scan("blue", before, after, bucketSize, Duration.Zero, math.max)
        .futureValue
        .map {
          case (persistenceId, (tagSeqNr, _)) => (persistenceId, tagSeqNr)
        }
      pidSequenceNrs should equal(Map("p1" -> 2, "p2" -> 6))
    }

    "be able to scan multiple buckets" in {
      val threeBucketsAgo = LocalDateTime.now(ZoneOffset.UTC).minusHours(3)
      writeTaggedEvent(threeBucketsAgo, PersistentRepr("p1e1", persistenceId = "p1"), Set("blue"), 1, bucketSize)
      writeTaggedEvent(threeBucketsAgo, PersistentRepr("p1e2", persistenceId = "p1"), Set("blue"), 2, bucketSize)

      val twoBucketsAgo = LocalDateTime.now(ZoneOffset.UTC).minusHours(2)
      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p2e1", persistenceId = "p2"), Set("blue"), 1, bucketSize)

      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p3e1", persistenceId = "p3"), Set("blue"), 5, bucketSize)
      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p3e1", persistenceId = "p3"), Set("blue"), 6, bucketSize)
      val before = UUIDs.startOf(threeBucketsAgo.toInstant(ZoneOffset.UTC).toEpochMilli)
      val after = UUIDs.timeBased()
      val pidSequenceNrs = queries.tagViewScanner.futureValue
        .scan("blue", before, after, bucketSize, Duration.Zero, math.max)
        .futureValue
        .map {
          case (persistenceId, (tagSeqNr, _)) => (persistenceId, tagSeqNr)
        }
      pidSequenceNrs should equal(Map("p1" -> 2, "p2" -> 1, "p3" -> 6))
    }
  }
}
