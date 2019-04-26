/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }

import akka.NotUsed
import akka.actor.ActorRef
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, Minute, TimeBucket }
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.persistence.journal.Tagged
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{ EventEnvelope, NoOffset }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.Session
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

object EventsByTagStageSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val fetchSize = 3L
  val config = ConfigFactory.parseString(s"""
        akka.loglevel = INFO

        akka.actor.serialize-messages=on

        cassandra-journal {
          log-queries = off
          events-by-tag {
           flush-interval = 0ms
           bucket-size = Minute
          }
        }

        cassandra-query-journal {
          first-time-bucket = "${today.minusMinutes(5).format(firstBucketFormatter)}"
          max-result-size-query = $fetchSize
          log-queries = on
          refresh-interval = 200ms
          events-by-tag {
            # Speeds up tests
            eventual-consistency-delay = 200ms
            gap-timeout = 3s
            new-persistence-id-scan-timeout = 500s
          }
        }
    """).withFallback(CassandraLifecycle.config)

}

class EventsByTagStageSpec
    extends CassandraSpec(EventsByTagStageSpec.config)
    with BeforeAndAfterAll
    with TestTagWriter
    with ImplicitSender {

  import EventsByTagStageSpec._

  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  val serialization: Serialization = SerializationExtension(system)

  lazy val session: Session = {
    import system.dispatcher
    Await.result(writePluginConfig.sessionProvider.connect(), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
    super.afterAll()
  }

  private val waitTime = 300.milliseconds // bigger than the eventual consistency delay
  private val longWaitTime = waitTime * 3
  private val bucketSize = Minute

  val noMsgTimeout = 100.millis

  "Current EventsByTag" must {

    "find new events" in {
      val ref = setup("p-1")
      send(ref, Tagged("e-1", Set("tag-2")))
      send(ref, Tagged("e-2", Set("tag-1")))
      send(ref, Tagged("e-3", Set("tag-2")))
      send(ref, Tagged("e-4", Set("tag-2")))

      val stream = queries.currentEventsByTag("tag-2", NoOffset)
      val sub = stream.toMat(TestSink.probe[EventEnvelope])(Keep.right).run()

      sub.request(2)
      sub.expectNoMessage(50.millis) // eventual consistency delay prevents events coming right away
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "e-3") => }
      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "e-4") => }
      sub.expectComplete()
    }

    "empty tag completes" in {
      val stream: Source[EventEnvelope, NotUsed] =
        queries.currentEventsByTag("bogus", NoOffset)
      val sub = stream.toMat(TestSink.probe[EventEnvelope])(Keep.right).run()
      sub.request(1)
      sub.expectComplete()
    }

    "find events - paging" in {
      val ref = setup("p-2")
      (1L to (fetchSize + 1)).foreach { i =>
        send(ref, Tagged(s"e-$i", Set("paged")))
      }

      val stream = queries.currentEventsByTag("paged", NoOffset)
      val sub = stream.runWith(TestSink.probe[EventEnvelope])

      sub.request(fetchSize + 1L)
      (1L to (fetchSize + 1)).foreach { i =>
        val id = s"e-$i"
        system.log.debug("Looking for event {}", id)
        sub.expectNextPF { case EventEnvelope(_, "p-2", `i`, `id`) => }
      }
    }

    "see events in previous time buckets" in {
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val lastBucket = nowTime.minusMinutes(1)
      writeTaggedEvent(lastBucket, PersistentRepr("e-1", 1, "p-3"), Set("CurrentPreviousBuckets"), 1, bucketSize)
      writeTaggedEvent(
        lastBucket.plusSeconds(1),
        PersistentRepr("e-2", 2, "p-3"),
        Set("CurrentPreviousBuckets"),
        2,
        bucketSize)

      val tagStream = queries.currentEventsByTag("CurrentPreviousBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-3", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-3", 2, "e-2") => }
      sub.expectComplete()
    }

    "find events in previous time buckets spanning multiple buckets" in {
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val twoBucketsAgo = nowTime.minusMinutes(2)
      val lastBucket = nowTime.minusMinutes(1)
      writeTaggedEvent(
        twoBucketsAgo,
        PersistentRepr("e-1", 1, "l-4"),
        Set("CurrentPreviousMultipleBuckets"),
        1,
        bucketSize)
      writeTaggedEvent(
        twoBucketsAgo.plusSeconds(1),
        PersistentRepr("e-2", 2, "l-4"),
        Set("CurrentPreviousMultipleBuckets"),
        2,
        bucketSize)
      writeTaggedEvent(
        lastBucket,
        PersistentRepr("e-3", 3, "l-4"),
        Set("CurrentPreviousMultipleBuckets"),
        3,
        bucketSize)
      writeTaggedEvent(
        lastBucket.plusSeconds(1),
        PersistentRepr("e-4", 4, "l-4"),
        Set("CurrentPreviousMultipleBuckets"),
        4,
        bucketSize)

      val tagStream = queries.currentEventsByTag("CurrentPreviousMultipleBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

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

      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4), bucketSize)

      val tagStream = queries.currentEventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(longWaitTime)

      // We still expect the timeuuid to be ordered correctly as it comes from the same node
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3), bucketSize)
      system.log.debug("p1e3 written: Offset: {}. Bucket: {}", times(3), TimeBucket(times(3), bucketSize))
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }

      sub.expectNoMessage(waitTime)
      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectComplete()
    }

    "timeout looking for missing events" in {
      val tag = "CurrentMissingEventTimeout"
      val times = (1 to 5).map { _ =>
        UUIDs.timeBased()
      }

      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4), bucketSize)

      val tagStream: Source[EventEnvelope, NotUsed] = queries.currentEventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(longWaitTime)

      sub.expectError().getMessage should equal(
        s"Unable to find missing tagged event: " +
        s"PersistenceId: p-1. Tag: $tag. TagPidSequenceNr: Set(3). Previous offset: ${times(1)}")
    }

    "find multiple missing messages that span time buckets" in {
      val tag = "CurrentMissingEventsInPreviousBucket"
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val twoBucketsAgo = nowTime.minusMinutes(2)
      val lastBucket = nowTime.minusMinutes(1)
      val thisBucket = nowTime

      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, bucketSize)
      writeTaggedEvent(thisBucket, PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, bucketSize)

      val tagStream = queries.currentEventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])
      sub.request(5)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      // make it switch between partitions a few times
      sub.expectNoMessage(longWaitTime)

      writeTaggedEvent(lastBucket, PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, bucketSize)
      sub.expectNoMessage(longWaitTime)

      writeTaggedEvent(lastBucket, PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, bucketSize)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectComplete()
    }

    "not look for initial missing events for offset queries starting in old time buckets" in {
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val twoBucketsAgo = nowTime.minusMinutes(2)
      val tag = "CurrentOffsetMissingInOldBucket"

      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p1e10", 10, "p-1"), Set(tag), 10, bucketSize)
      writeTaggedEvent(nowTime, PersistentRepr("p1e11", 11, "p-1"), Set(tag), 11, bucketSize)

      val tagStream = queries.currentEventsByTag(
        tag,
        queries.timeBasedUUIDFrom(twoBucketsAgo.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli))
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(3)
      sub.expectNextWithTimeoutPF(waitTime, { case EventEnvelope(_, "p-1", 10, "p1e10") => })
      sub.expectNextWithTimeoutPF(waitTime, { case EventEnvelope(_, "p-1", 11, "p1e11") => })
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

      val sub = stream.toMat(TestSink.probe[EventEnvelope])(Keep.right).run()
      sub.request(1)
      sub.expectNoMessage(waitTime)
    }

    "find new events" in {
      val ref = setup("b")
      send(ref, Tagged("e-1", Set("tag-3")))

      val blackSrc = queries.eventsByTag(tag = "tag-3", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe[EventEnvelope])
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
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val twoBucketsAgo = nowTime.minusMinutes(2)
      val lastBucket = nowTime.minusMinutes(1)
      writeTaggedEvent(twoBucketsAgo, PersistentRepr("e-1", 1, "l-4"), Set("LivePreviousBuckets"), 1, bucketSize)
      writeTaggedEvent(
        twoBucketsAgo.plusSeconds(1),
        PersistentRepr("e-2", 2, "l-4"),
        Set("LivePreviousBuckets"),
        2,
        bucketSize)
      writeTaggedEvent(lastBucket, PersistentRepr("e-3", 3, "l-4"), Set("LivePreviousBuckets"), 3, bucketSize)
      writeTaggedEvent(
        lastBucket.plusSeconds(1),
        PersistentRepr("e-4", 4, "l-4"),
        Set("LivePreviousBuckets"),
        4,
        bucketSize)

      val tagStream = queries.eventsByTag("LivePreviousBuckets", NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 2, "e-2") => }
      sub.expectNoMessage(waitTime)

      sub.request(2)
      sub.expectNextPF { case EventEnvelope(_, "l-4", 3, "e-3") => }
      sub.expectNextPF { case EventEnvelope(_, "l-4", 4, "e-4") => }

      writeTaggedEvent(nowTime, PersistentRepr("e-5", 5, "l-4"), Set("LivePreviousBuckets"), 5, bucketSize)
      writeTaggedEvent(
        nowTime.plusSeconds(1),
        PersistentRepr("e-6", 6, "l-4"),
        Set("LivePreviousBuckets"),
        6,
        bucketSize)

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
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4), bucketSize)

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(longWaitTime)

      // We still expect the timeuuid to be ordered correctly as it comes from the same node
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3), bucketSize)
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
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, times(1), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(2), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e6", 6, "p-1"), Set(tag), 6, times(6), bucketSize)

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(4)
      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNoMessage(waitTime)

      // make the middle one available
      writeTaggedEvent(PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, times(4), bucketSize)
      writeTaggedEvent(PersistentRepr("p1e5", 5, "p-1"), Set(tag), 5, times(5), bucketSize)
      sub.expectNoMessage(waitTime)

      // now all the missing are available
      writeTaggedEvent(PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, times(3), bucketSize)
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
      writeTaggedEvent(PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, times(0), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e3", 3, "p-2"), Set(tag), 3, times(3), bucketSize)

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])
      sub.request(10)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      sub.expectNoMessage(waitTime)

      writeTaggedEvent(PersistentRepr("p2e2", 2, "p-2"), Set(tag), 2, times(2), bucketSize)
      sub.expectNoMessage(waitTime)

      writeTaggedEvent(PersistentRepr("p2e1", 1, "p-2"), Set(tag), 1, times(1), bucketSize)

      sub.expectNextPF { case EventEnvelope(_, "p-2", 1, "p2e1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 2, "p2e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 3, "p2e3") => }

      writeTaggedEvent(PersistentRepr("p2e4", 4, "p-2"), Set(tag), 4, times(4), bucketSize)
      writeTaggedEvent(PersistentRepr("p2e5", 5, "p-2"), Set(tag), 5, UUIDs.timeBased(), bucketSize)
      sub.expectNextPF { case EventEnvelope(_, "p-2", 4, "p2e4") => }
      sub.expectNextPF { case EventEnvelope(_, "p-2", 5, "p2e5") => }

      sub.cancel()
    }

    "find missing first event in previous bucket" in {
      val tag = "LiveMissingFirstInPreviousBucket"
      val tagStream = queries.eventsByTag(tag, NoOffset)
      val (nowTime, previousBucket) = currentAndPreviousBucket()

      writeTaggedEvent(nowTime.plusSeconds(1), PersistentRepr("e-3", 3, "p-1"), Set(tag), 3, bucketSize)

      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])
      sub.request(5)
      sub.expectNoMessage(waitTime)

      writeTaggedEvent(previousBucket, PersistentRepr("e-1", 1, "p-1"), Set(tag), 1, bucketSize)
      writeTaggedEvent(nowTime, PersistentRepr("e-2", 2, "p-1"), Set(tag), 2, bucketSize)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "e-1") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "e-2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "e-3") => }
    }

    "find multiple missing messages that span time buckets" in {
      val tag = "LiveMissingEventsInPreviousBucket"
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val twoBucketsAgo = nowTime.minusMinutes(2)
      val lastBucket = nowTime.minusMinutes(1)
      val thisBucket = nowTime

      writeTaggedEvent(twoBucketsAgo, PersistentRepr("p1e1", 1, "p-1"), Set(tag), 1, bucketSize)
      writeTaggedEvent(thisBucket, PersistentRepr("p1e4", 4, "p-1"), Set(tag), 4, bucketSize)

      val tagStream = queries.eventsByTag(tag, NoOffset)
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])
      sub.request(5)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 1, "p1e1") => }
      // make it switch between partitions a few times
      sub.expectNoMessage(longWaitTime)

      writeTaggedEvent(lastBucket, PersistentRepr("p1e2", 2, "p-1"), Set(tag), 2, bucketSize)
      sub.expectNoMessage(longWaitTime)

      writeTaggedEvent(lastBucket, PersistentRepr("p1e3", 3, "p-1"), Set(tag), 3, bucketSize)

      sub.expectNextPF { case EventEnvelope(_, "p-1", 2, "p1e2") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 3, "p1e3") => }
      sub.expectNextPF { case EventEnvelope(_, "p-1", 4, "p1e4") => }
      sub.expectNoMessage(waitTime)

      sub.cancel()
    }

    "look for events in the initial bucket for only a short time" in {
      val nowTime = LocalDateTime.now(ZoneOffset.UTC)
      val tag = "CurrentOffsetMissingInInitialBucket"

      val tagStream =
        queries.eventsByTag(
          tag,
          queries.timeBasedUUIDFrom(nowTime.minusSeconds(1).toInstant(ZoneOffset.UTC).toEpochMilli))
      val sub = tagStream.runWith(TestSink.probe[EventEnvelope])

      sub.request(4)
      sub.expectNoMessage(100.millis)
      writeTaggedEvent(nowTime, PersistentRepr("p1e11", 11, "p-1"), Set(tag), 11, bucketSize)
      // wait more than the new persistence id timeout but less than the gap-timeout
      sub.expectNextWithTimeoutPF(2.seconds, { case EventEnvelope(_, "p-1", 11, "p1e11") => })
      sub.cancel()
    }
  }

  private def currentAndPreviousBucket(): (LocalDateTime, LocalDateTime) = {
    def nowTime(): LocalDateTime = {
      val now = LocalDateTime.now(ZoneOffset.UTC)
      // If too close to the end of bucket the query might span 3 buckets and events by tag only goes
      // back one bucket
      if (now.getSecond >= 58) {
        Thread.sleep(1000)
        nowTime()
      } else now
    }

    val now = nowTime()
    (now, now.minusMinutes(1))
  }

  private def send(ref: ActorRef, a: Tagged): Unit = {
    ref ! a
    expectMsg(waitTime, s"${a.payload}-done")
  }

  private def setup(persistenceId: String): ActorRef =
    system.actorOf(TestActor.props(persistenceId))
}
