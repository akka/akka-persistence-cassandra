/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ Cluster, Session }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.Await
import scala.concurrent.duration._

object EventsByTagStageSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val fetchSize: Int = 3
  val config = ConfigFactory.parseString(
    s"""
       | akka.loglevel = DEBUG
       |
       | akka.actor.serialize-messages=off
       |
       | cassandra-journal {
       |   keyspace=eventsbytagstagespec
       |   log-queries = off
       |   events-by-tag {
       |    flush-interval = 0ms
       |    bucket-size = Day
       |   }
       | }
       |
       | cassandra-query-journal {
       |   first-time-bucket = ${today.minusDays(5).format(firstBucketFormat)}
       |   max-result-size-query = $fetchSize
       |   log-queries = on
       | }
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

}

class EventsByTagStageSpec
  extends TestKit(ActorSystem("EventsByTagStageSpec", EventsByTagStageSpec.config))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers
  with BeforeAndAfterAll
  with TestTagWriter {

  import EventsByTagStageSpec._

  override def systemName: String = "EventsByTagStageSpec"

  override def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .withClusterName("EventsByTagStageSpecCleanup")
      .addContactPoint("localhost")
      .withPort(9042)
      .build()
    cluster.connect().execute("drop keyspace eventsbytagstagespec")
    cluster.close()
  }

  val today = LocalDateTime.now(ZoneOffset.UTC)
  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  val serialization: Serialization = SerializationExtension(system)
  lazy val session: Session = {
    import system.dispatcher
    Await.result(writePluginConfig.sessionProvider.connect(), 5.seconds)
  }

  implicit val mat = ActorMaterializer()(system)
  private val waitTime = 500.milliseconds
  private val longWaitTime = 1000.milliseconds

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val noMsgTimeout = 100.millis

  "Current EventsByTag" must {

    "find new events" in {
      val ref = setup("p-1")
      send(ref, Tagged("e-1", Set("tag-2")))
      send(ref, Tagged("e-2", Set("tag-1")))
      send(ref, Tagged("e-3", Set("tag-2")))
      send(ref, Tagged("e-4", Set("tag-2")))

      val stream = queries.currentEventsByTag("tag-2", NoOffset)
      val sub = stream.toMat(TestSink.probe)(Keep.right).run()

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "e-3") => }
      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "e-4") => }
      sub.expectComplete()
    }

    "empty tag completes" in {
      val stream: Source[EventEnvelope, NotUsed] =
        queries.currentEventsByTag("bogus", NoOffset)
      val sub = stream.toMat(TestSink.probe)(Keep.right).run()
      sub.request(1)
      sub.expectComplete()
    }

    "find events - paging" in {
      val ref = setup("p-2")
      (1 to (fetchSize + 1)).foreach { i =>
        send(ref, Tagged(s"e-$i", Set("paged")))
      }

      val stream = queries.currentEventsByTag("paged", NoOffset)
      val sub = stream.runWith(TestSink.probe)

      sub.request(fetchSize + 1)
      (1 to (fetchSize + 1)).foreach { i =>
        val id = s"e-$i"
        sub.expectNextPF { case EventEnvelope(_, "p-2", `i`, `id`) => }
      }
    }

    "see events in previous time buckets" in {
      val lastBucket = today.minusDays(1)
      writeTaggedEvent(lastBucket, PersistentRepr("e-1", 1, "p-3"), Set("CurrentPreviousBuckets"), 1)
      writeTaggedEvent(lastBucket.plusSeconds(1), PersistentRepr("e-2", 2, "p-3"), Set("CurrentPreviousBuckets"), 2)

      val tagStream = queries.currentEventsByTag("CurrentPreviousBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-3", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-3", 2, "e-2") => }
      sub.expectComplete()
    }

    "find events in previous time buckets spanning multiple buckets" in {
      val twoBucketsAgo = today.minusDays(2)
      val lastBucket = today.minusDays(1)
      writeTaggedEvent(twoBucketsAgo, PersistentRepr("e-1", 1, "l-4"), Set("CurrentPreviousMultipleBuckets"), 1)
      writeTaggedEvent(twoBucketsAgo.plusSeconds(1), PersistentRepr("e-2", 2, "l-4"), Set("CurrentPreviousMultipleBuckets"), 2)
      writeTaggedEvent(lastBucket, PersistentRepr("e-3", 3, "l-4"), Set("CurrentPreviousMultipleBuckets"), 3)
      writeTaggedEvent(lastBucket.plusSeconds(1), PersistentRepr("e-4", 4, "l-4"), Set("CurrentPreviousMultipleBuckets"), 4)

      val tagStream = queries.currentEventsByTag("CurrentPreviousMultipleBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 2, "e-2") => }
      sub.expectNoMessage(waitTime)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 3, "e-3") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 4, "e-4") => }
      sub.expectComplete()
    }

    "find missing event" in {
      val tag = "CurrentMissingEvent"
      val times = (1 to 5).map { _ =>
        UUIDs.timeBased()
      }

      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0))
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1))
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2))
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4))

      val tagStream = queries.currentEventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(longWaitTime)

      // We still expect the timeuuid to be ordered correctly as it comes from the same node
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3))
      system.log.debug("p1e3 written")
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }

      sub.expectNoMessage(waitTime)
      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectComplete()
    }
  }

  "Live EventsByTag" must {
    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "empty tag should not complete" in {
      val stream: Source[EventEnvelope, NotUsed] =
        queries.eventsByTag("bogus", NoOffset)

      val sub = stream.toMat(TestSink.probe)(Keep.right).run()
      sub.request(1)
      sub.expectNoMessage(waitTime)
    }

    "find new events" in {
      val ref = setup("b")
      send(ref, Tagged("e-1", Set("tag-3")))

      val blackSrc = queries.eventsByTag(tag = "tag-3", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe)
      probe.request(2)
      probe.expectNextPF { case EventEnvelope(_, "b", 1L, "e-1") => }
      probe.expectNoMessage(waitTime)

      send(ref, Tagged("e-2", Set("tag-3")))
      probe.expectNextPF { case EventEnvelope(_, "b", 2L, "e-2") => }

      send(ref, Tagged("e-3", Set("tag-3")))
      probe.expectNoMessage(waitTime)

      probe.request(1)
      probe.expectNextPF { case EventEnvelope(_, "b", 3L, "e-3") => }

      probe.cancel()
    }

    "find events in previous time buckets spanning multiple buckets" in {
      val twoBucketsAgo = today.minusDays(2)
      val lastBucket = today.minusDays(1)
      writeTaggedEvent(twoBucketsAgo, PersistentRepr("e-1", 1, "l-4"), Set("LivePreviousBuckets"), 1)
      writeTaggedEvent(twoBucketsAgo.plusSeconds(1), PersistentRepr("e-2", 2, "l-4"), Set("LivePreviousBuckets"), 2)
      writeTaggedEvent(lastBucket, PersistentRepr("e-3", 3, "l-4"), Set("LivePreviousBuckets"), 3)
      writeTaggedEvent(lastBucket.plusSeconds(1), PersistentRepr("e-4", 4, "l-4"), Set("LivePreviousBuckets"), 4)

      val tagStream = queries.eventsByTag("LivePreviousBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 2, "e-2") => }
      sub.expectNoMessage(waitTime)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 3, "e-3") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 4, "e-4") => }

      writeTaggedEvent(today, PersistentRepr("e-5", 5, "l-4"), Set("LivePreviousBuckets"), 5)
      writeTaggedEvent(today.plusSeconds(1), PersistentRepr("e-6", 6, "l-4"), Set("LivePreviousBuckets"), 6)

      sub.expectNoMessage(waitTime)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 5, "e-5") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 6, "e-6") => }
      sub.cancel()
    }

    "find missing event" in {
      val tag = "LiveMissingEvent"
      val times = (1 to 5).map { _ =>
        UUIDs.timeBased()
      }
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0))
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1))
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2))
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4))

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(longWaitTime)

      // We still expect the timeuuid to be ordered correctly as it comes from the same node
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3))
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }
      sub.expectNoMessage(waitTime)
      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectNoMessage(waitTime)
      sub.cancel()
    }

    "find multiple missing events" in {
      val tag = "LiveMultipleMissingEvent"
      val times = (1 to 7).map { _ =>
        UUIDs.timeBased()
      }
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0))
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1))
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2))
      writeTaggedEvent(PersistentRepr("p1e6", 6, "p-1"), Set(tag), 6, times(6))

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe)

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(waitTime)

      // make the middle one available
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4))
      writeTaggedEvent(PersistentRepr("p1e5", 5, "p-1"), Set(tag), 5, times(5))
      sub.expectNoMessage(waitTime)

      // now all the missing are available
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3))
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }
      sub.expectNoMessage(waitTime)
      sub.request(3)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 5, "p1e5") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 6, "p1e6") => }
      sub.expectNoMessage(waitTime)
      sub.cancel()
    }

    "find missing first event" in {
      val tag = "LiveMissingFirstEvent"
      val times = (1 to 5).map { _ =>
        UUIDs.timeBased()
      }
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0))
      writeTaggedEvent(PersistentRepr("p2e3", 3, "p-2"), Set(tag), 3, times(3))

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe)
      sub.request(5)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNoMessage(waitTime)

      writeTaggedEvent(PersistentRepr("p2e2", 2, "p-2"), Set(tag), 2, times(2))
      sub.expectNoMessage(waitTime)

      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(1))

      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 2, "p2e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 3, "p2e3") => }

      writeTaggedEvent(PersistentRepr("p2e4", 4, "p-2"), Set(tag), 4, times(4))
      sub.expectNextPF { case EventEnvelope(_, "p-2", 4, "p2e4") => }

      sub.cancel()
    }

    "multiple missing messages that span time buckets" in {
      pending
    }

    "starting at offset, missing events, track back to offset for first event" in {
      pending
    }

    "no offset, missing events, track back a configured amount for first event" in {
      pending
    }
  }

  private def send(ref: ActorRef, a: Tagged): Unit = {
    ref ! a
    expectMsg(waitTime, a.payload + "-done")
  }

  private def setup(persistenceId: String): ActorRef = {
    system.actorOf(TestActor.props(persistenceId))
  }
}
