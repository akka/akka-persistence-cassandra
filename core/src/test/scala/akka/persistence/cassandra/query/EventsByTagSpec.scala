/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.temporal.ChronoUnit
import java.time.{ LocalDateTime, ZoneOffset }
import java.util.UUID

import akka.actor.{ PoisonPill, Props }
import akka.event.Logging.Warning
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, CassandraStatements, Day }
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, EventWithMetaData }
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.scaladsl.{ CurrentEventsByTagQuery, EventsByTagQuery }
import akka.persistence.query.{ EventEnvelope, NoOffset, TimeBasedUUID }
import akka.persistence.{ PersistentActor, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object EventsByTagSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(
    s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages = off
    akka.actor.warn-about-java-serializer-usage = off
    cassandra-journal {
      #target-partition-size = 5

      event-adapters {
        color-tagger  = akka.persistence.cassandra.query.ColorFruitTagger
        metadata-tagger  = akka.persistence.cassandra.query.EventWithMetaDataTagger
      }

      event-adapter-bindings = {
        "java.lang.String" = color-tagger
        "akka.persistence.cassandra.EventWithMetaData" = metadata-tagger
      }

      events-by-tag {
        flush-interval = 0ms
        bucket-size = Day
        time-to-live = 1d
      }
    }

    cassandra-query-journal {
      refresh-interval = 500ms
      max-buffer-size = 50
      first-time-bucket = "${today.minusDays(5).format(firstBucketFormatter)}"
      events-by-tag {
        eventual-consistency-delay = 2s
      }
    }
    """).withFallback(CassandraLifecycle.config)

  val strictConfig = ConfigFactory.parseString(
    s"""
    akka.loglevel = INFO
    cassandra-query-journal {
      refresh-interval = 100ms
      events-by-tag {
        gap-timeout = 5s
        new-persistence-id-scan-timeout = 200s
      }

    }
    """).withFallback(config)

  val strictConfigFirstOffset1001DaysAgo = ConfigFactory.parseString(
    s"""
    cassandra-query-journal.first-time-bucket = "${today.minusDays(1001).format(firstBucketFormatter)}"
    """).withFallback(strictConfig)

  val disabledConfig = ConfigFactory.parseString(
    """
      akka.loglevel = INFO
      cassandra-journal {
        keyspace=EventsByTagDisabled
        events-by-tag.enabled = false
      }
    """).withFallback(config)
}

class EventWithMetaDataTagger extends WriteEventAdapter {
  override def manifest(event: Any) = ""
  override def toJournal(event: Any) = event match {
    case evm: EventWithMetaData =>
      Tagged(evm, Set("gotmeta"))
    case _ => event
  }
}

class ColorFruitTagger extends WriteEventAdapter {
  val colors = Set("green", "black", "blue", "yellow")
  val fruits = Set("apple", "banana")
  override def toJournal(event: Any): Any = event match {
    case s: String =>
      val colorTags = colors.foldLeft(Set.empty[String])((acc, c) => if (s.contains(c)) acc + c else acc)
      val fruitTags = fruits.foldLeft(Set.empty[String])((acc, c) => if (s.contains(c)) acc + c else acc)
      val tags = colorTags union fruitTags
      if (tags.isEmpty) event
      else Tagged(event, tags)
    case _ => event
  }

  override def manifest(event: Any): String = ""
}

abstract class AbstractEventsByTagSpec(config: Config) extends CassandraSpec(config) with BeforeAndAfterEach
  with TestTagWriter {

  val bucketSize = Day
  val waitTime = 100.millis

  val serialization = SerializationExtension(system)
  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))

  lazy val session = {
    import system.dispatcher
    Await.result(writePluginConfig.sessionProvider.connect(), 5.seconds)
  }

  lazy val preparedWriteMessage = {
    val writeStatements: CassandraStatements = new CassandraStatements {
      def config: CassandraJournalConfig = writePluginConfig
    }
    session.prepare(writeStatements.writeMessage(withMeta = false))
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    Try(session.getCluster.close())
    super.afterAll()
  }

  val logProbe = TestProbe()
  system.eventStream.subscribe(logProbe.ref, classOf[Warning])
  system.eventStream.subscribe(logProbe.ref, classOf[Error])

  override protected def afterEach(): Unit = {
    // check for the buffer exceeded log (and other issues)
    logProbe.expectNoMessage(waitTime)
    super.afterEach()
  }
}

class EventsByTagSpec extends AbstractEventsByTagSpec(EventsByTagSpec.config) {

  import EventsByTagSpec._

  "Cassandra query currentEventsByTag" must {
    "set ttl on table" in {
      session.getCluster.getMetadata.getKeyspace(journalName).getTable("tag_views").getOptions.getDefaultTimeToLive shouldEqual 86400
    }

    "implement standard CurrentEventsByTagQuery" in {
      queries.isInstanceOf[CurrentEventsByTagQuery] should ===(true)
    }

    "find existing events" in {
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      a ! "hello"
      expectMsg(20.seconds, s"hello-done")
      a ! "a green apple"
      expectMsg(s"a green apple-done")
      b ! "a black car"
      expectMsg(s"a black car-done")
      a ! "something else"
      expectMsg(s"something else-done")
      a ! "a green banana"
      expectMsg(s"a green banana-done")
      b ! "a green leaf"
      expectMsg(s"a green leaf-done")

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
      probe.expectNoMessage(500.millis)
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe.expectComplete()

      val blackSrc = queries.currentEventsByTag(tag = "black", offset = NoOffset)
      val probe2 = blackSrc.runWith(TestSink.probe[Any])
      probe2.request(5)
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
      probe2.expectComplete()

      val appleSrc = queries.currentEventsByTag(tag = "apple", offset = NoOffset)
      val probe3 = appleSrc.runWith(TestSink.probe[Any])
      probe3.request(5)
      probe3.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe3.expectComplete()
    }

    "complete when no events" in {
      val src = queries.currentEventsByTag(tag = "pink", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectComplete()
    }

    "not see new events after demand request" in {
      val c = system.actorOf(TestActor.props("c"))

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
      probe.expectNoMessage(waitTime)

      c ! "a green cucumber"
      expectMsg(s"a green cucumber-done")

      probe.expectNoMessage(waitTime)
      probe.request(5)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe.expectComplete() // green cucumber not seen
    }

    "find events from timestamp offset" in {
      val greenSrc1 = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      val appleOffs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 2L, "a green apple") => e
      }.offset.asInstanceOf[TimeBasedUUID]
      val bananaOffs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "a green banana") => e
      }.offset.asInstanceOf[TimeBasedUUID]
      probe1.cancel()

      val appleTimestamp = queries.timestampFrom(appleOffs)
      val bananaTimestamp = queries.timestampFrom(bananaOffs)
      appleTimestamp should be <= bananaTimestamp

      val greenSrc2 = queries.currentEventsByTag(tag = "green", queries.timeBasedUUIDFrom(bananaTimestamp))
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      if (appleTimestamp == bananaTimestamp)
        probe2.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.cancel()
    }

    "find events from UUID offset" in {
      val greenSrc1 = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }.offset
      probe1.cancel()

      val greenSrc2 = queries.currentEventsByTag(tag = "green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.cancel()
    }

    "find existing events that spans several time buckets" in {
      val t1 = today.minusDays(5).toLocalDate.atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t1, pr1, Set("T1-current"), 1, bucketSize)
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t2, pr2, Set("T1-current"), 2, bucketSize)
      val t3 = t1.plusDays(1)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t3, pr3, Set("T1-current"), 3, bucketSize)
      val t4 = t1.plusDays(3)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t4, pr4, Set("T1-current"), 4, bucketSize)

      val src = queries.currentEventsByTag(tag = "T1-current", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }
      probe.expectNoMessage(500.millis)
      probe.request(5)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.expectComplete()
    }
  }

  "Cassandra live eventsByTag" must {
    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "find new events" in {
      val d = system.actorOf(TestActor.props("d"))

      val blackSrc = queries.eventsByTag(tag = "black", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
      probe.expectNoMessage(waitTime)

      d ! "a black dog"
      expectMsg(s"a black dog-done")
      d ! "a black night"
      expectMsg(s"a black night-done")

      probe.expectNextPF { case e @ EventEnvelope(_, "d", 1L, "a black dog") => e }
      probe.expectNoMessage(waitTime)
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "d", 2L, "a black night") => e }
      probe.cancel()
    }

    "find events from timestamp offset" in {
      val greenSrc1 = queries.eventsByTag(tag = "green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      val appleOffs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 2L, "a green apple") => e
      }.offset.asInstanceOf[TimeBasedUUID]
      val bananaOffs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "a green banana") => e
      }.offset.asInstanceOf[TimeBasedUUID]
      probe1.cancel()

      val appleTimestamp = queries.timestampFrom(appleOffs)
      val bananaTimestamp = queries.timestampFrom(bananaOffs)
      bananaTimestamp should be <= bananaTimestamp

      val greenSrc2 = queries.eventsByTag(tag = "green", queries.timeBasedUUIDFrom(bananaTimestamp))
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      if (appleTimestamp == bananaTimestamp)
        probe2.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
      probe2.expectNoMessage(waitTime)
      probe2.cancel()
    }

    "find events from UUID offset " in {
      val greenSrc1 = queries.eventsByTag(tag = "green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }.offset
      probe1.cancel()

      val greenSrc2 = queries.eventsByTag(tag = "green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
      probe2.expectNoMessage(waitTime)
      probe2.cancel()
    }

    "find new events that spans several time buckets" in {
      val t1 = today.minusDays(5).toLocalDate.atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t1, pr1, Set("T1-live"), 1, bucketSize)
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t2, pr2, Set("T1-live"), 2, bucketSize)

      val src = queries.eventsByTag(tag = "T1-live", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }

      val t3 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t3, pr3, Set("T1-live"), 3, bucketSize)
      val t4 = LocalDateTime.now(ZoneOffset.UTC)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t4, pr4, Set("T1-live"), 4, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.cancel()
    }

    // note that the test below works if another event from p2 to come
    // then we go back and look for e1 from p1
    "sort events by timestamp" ignore {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusSeconds(10)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("p1-e1", 1L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t1, pr1, Set("T2"), 1, bucketSize)
      val t3 = LocalDateTime.now(ZoneOffset.UTC)
      val pr3 = PersistentRepr("p1-e2", 2L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t3, pr3, Set("T2"), 2, bucketSize)

      val src = queries.eventsByTag(tag = "T2", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)

      // simulate async eventually consistent Materialized View update
      // that cause p1-e2 to show up before p2-e1
      Thread.sleep(500)
      val t2 = t3.minus(1, ChronoUnit.MILLIS)
      val pr2 = PersistentRepr("p2-e1", 1L, "p2", "", writerUuid = w2)
      writeTaggedEvent(t2, pr2, Set("T2"), 1, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "p1-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 1L, "p2-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "p1-e2") => e }
      probe.cancel()
    }

    "stream many events" in {
      val e = system.actorOf(TestActor.props("e"))

      val src = queries.eventsByTag(tag = "yellow", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      for (n <- 1 to 100)
        e ! s"yellow-$n"

      probe.request(200)
      for (n <- 1 to 100) {
        val Expected = s"yellow-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMessage(waitTime)

      for (n <- 101 to 200)
        e ! s"yellow-$n"

      for (n <- 101 to 200) {
        val Expected = s"yellow-$n"
        withClue(s"Expected: $Expected") {
          probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
        }
      }
      probe.expectNoMessage(waitTime)

      probe.request(10)
      probe.expectNoMessage(waitTime)
    }

    "return events with their metadata" in {
      val w1 = system.actorOf(TestActor.props("W1"))
      val src = queries.eventsByTag(tag = "gotmeta", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      w1 ! EventWithMetaData("e1", "this is such a good message")

      probe.request(2)
      probe.expectNextPF { case EventEnvelope(_, "W1", 1L, EventWithMetaData("e1", "this is such a good message")) => }
      probe.expectNoMessage(waitTime)
      probe.cancel()
    }
  }
}

class EventsByTagZeroEventualConsistencyDelaySpec
  extends AbstractEventsByTagSpec(
    ConfigFactory.parseString("cassandra-query-journal.eventual-consistency-delay = 0s")
      .withFallback(EventsByTagSpec.strictConfig)) {

  "Cassandra query currentEventsByTag with zero eventual-consistency-delay" must {

    "find existing events" in {
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      a ! "a green apple"
      val NumberOfBananas = 5017
      expectMsg(20.seconds, s"a green apple-done")
      (1 to NumberOfBananas).foreach { n =>
        a ! s"a green banana-$n"
        expectMsg(s"a green banana-$n-done")
      }

      b ! "a green leaf"
      expectMsg(s"a green leaf-done")

      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(NumberOfBananas + 10L)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "a green apple") => e }
      (1 to NumberOfBananas).foreach { n =>
        val SeqNr = 1L + n
        val Description = s"a green banana-$n"
        withClue(s"banana-$n") {
          probe.expectNextPF { case e @ EventEnvelope(_, "a", SeqNr, Description) => e }
        }
      }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a green leaf") => e }
      probe.expectComplete()
    }

  }
}

// Manually writing events for same persistence id is no longer valid
// tho new persistence ids from a node with a slow clocks is still applicable
// and we pick them up by noticing that there is a tagPidSequenceNr gap
class EventsByTagFindDelayedEventsSpec extends AbstractEventsByTagSpec(EventsByTagSpec.strictConfig) {
  "Cassandra live eventsByTag delayed messages" must {

    // slightly lower guarantee than before, we need another event to come along for that pid/tag combination
    "find delayed events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val p1e1 = PersistentRepr("p1-e1", 1L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t1, p1e1, Set("T6"), 1, bucketSize)

      val t2 = t1.plusSeconds(2)
      val p2e1 = PersistentRepr("p2-e1", 1L, "p2", "", writerUuid = w2)
      writeTaggedEvent(t2, p2e1, Set("T6"), 1, bucketSize)

      val src = queries.eventsByTag(tag = "T6", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "p1-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 1L, "p2-e1") => e }

      // delayed, and timestamp is before p2-e1
      val t3 = t1.plusSeconds(1)
      val p1e2 = PersistentRepr("p1-e2", 2L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t3, p1e2, Set("T6"), 2, bucketSize)
      val p1e3 = PersistentRepr("p1-e3", 3L, "p1", "", writerUuid = w1)
      writeTaggedEvent(t2.plusSeconds(1), p1e3, Set("T6"), 3, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "p1-e2") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "p1-e3") => e }

      probe.cancel()
    }

    "find delayed events 2" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val t2 = t1.plusSeconds(1)
      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTaggedEvent(t2, eventA1, Set("T7"), 1, bucketSize)

      val src = queries.eventsByTag(tag = "T7", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "A1") => e }

      // delayed, timestamp is before A1
      val eventB1 = PersistentRepr("B1", 1L, "b", "", writerUuid = w2)
      writeTaggedEvent(t1, eventB1, Set("T7"), 1, bucketSize)
      val t3 = t1.plusSeconds(2)
      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTaggedEvent(t3, eventB2, Set("T7"), 2, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe.cancel()
    }

    "find delayed events 3" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val eventB0 = PersistentRepr("B0", 1L, "b", "", writerUuid = w2)
      writeTaggedEvent(t1.minusSeconds(1), eventB0, Set("T8"), 1, bucketSize)

      val t2 = t1.plusSeconds(1)
      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTaggedEvent(t2, eventA1, Set("T8"), 1, bucketSize)

      val src = queries.eventsByTag(tag = "T8", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B0") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "A1") => e }

      // delayed, timestamp is before A1
      val eventB1 = PersistentRepr("B1", 2L, "b", "", writerUuid = w2)
      writeTaggedEvent(t1, eventB1, Set("T8"), 2, bucketSize)
      val t3 = t1.plusSeconds(2)
      val eventB2 = PersistentRepr("B2", 3L, "b", "", writerUuid = w2)
      writeTaggedEvent(t3, eventB2, Set("T8"), 3, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 3L, "B2") => e }

      probe.cancel()
    }

    "find delayed events from offset" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTaggedEvent(t1.plusSeconds(2), eventA1, Set("T9"), 1, bucketSize)

      val src1 = queries.eventsByTag(tag = "T9", offset = NoOffset)
      val probe1 = src1.runWith(TestSink.probe[Any])
      probe1.request(10)
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "A1") => e }.offset.asInstanceOf[TimeBasedUUID]
      probe1.cancel()

      // start a new query from the offset
      val src2 = queries.eventsByTag(tag = "T9", offset = offs)
      val probe2 = src2.runWith(TestSink.probe[Any])
      probe2.request(10)

      // delayed, timestamp is before A1, i.e. before the offset
      val eventB1 = PersistentRepr("B1", 1L, "b", "", writerUuid = w2)
      writeTaggedEvent(t1.plusSeconds(1), eventB1, Set("T9"), 1, bucketSize)
      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTaggedEvent(t1.plusSeconds(3), eventB2, Set("T9"), 2, bucketSize)

      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe2.cancel()
    }

    // Not supported atm as it requires us to back track without seeing a future event
    // for a new persistenceId
    "find delayed events when many other events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      (1L to 100L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        writeTaggedEvent(t1.plus(n, ChronoUnit.MILLIS), eventA, Set("T10"), n, bucketSize)
      }

      val src = queries.eventsByTag(tag = "T10", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(1000)
      probe.expectNextN(100)

      val t2 = t1.plusSeconds(1)
      (101L to 200L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        writeTaggedEvent(t2.plus(n, ChronoUnit.MILLIS), eventA, Set("T10"), n, bucketSize)
      }

      // delayed, timestamp is before A101 but after A100
      val eventB1 = PersistentRepr("B1", 1L, "b", "", writerUuid = w2)
      writeTaggedEvent(t2.minus(100, ChronoUnit.MILLIS), eventB1, Set("T10"), 1, bucketSize)

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B1") => e }

      // Now A101 - A200 can be delivered
      probe.expectNextN(100)

      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTaggedEvent(t2.plusSeconds(1), eventB2, Set("T10"), 2, bucketSize)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe.cancel()
    }

    "find events from many persistenceIds" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC)

      val src = queries.eventsByTag(tag = "T11", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(1000)

      // A101-2 to A200-2
      (101L to 200L).foreach { n =>
        val eventA2 = PersistentRepr(s"A$n-2", sequenceNr = 2, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTaggedEvent(t1.plus(500 + n, ChronoUnit.MILLIS), eventA2, Set("T11"), 1, bucketSize)
      }

      // limit is 50, so let's use something not divisible by 50
      // A1-2 to A70-2
      (1L to 70L).foreach { n =>
        val eventA2 = PersistentRepr(s"A$n-2", sequenceNr = 2, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTaggedEvent(t1.plus(n, ChronoUnit.MILLIS), eventA2, Set("T11"), 1, bucketSize)
      }

      (1L to 70L).foreach { n =>
        val ExpectedPid = s"a$n"
        withClue(s"Expected: $ExpectedPid") {
          probe.expectNextPF { case e @ EventEnvelope(_, ExpectedPid, 2L, _) => e }
        }
      }

      // Come after due to eventual consistency delay
      (101L to 200L).foreach { n =>
        val Expected = s"a$n"
        probe.expectNextPF { case e @ EventEnvelope(_, Expected, 2L, _) => e }
      }

      probe.expectNoMessage(1.second)
      probe.cancel()
    }

  }

}

class EventsByTagStrictBySeqNoEarlyFirstOffsetSpec
  extends AbstractEventsByTagSpec(EventsByTagSpec.strictConfigFirstOffset1001DaysAgo) {

  "Cassandra live eventsByTag with delayed-event-timeout > 0s and firstOffset = 1000 days ago" must {
    "find all events when starting the query 1000 days ago" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5).minusDays(1001)
      val t2 = t1.minusMinutes(1)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      // create two events per day over the last 1000 days to be sure that delayed event backtracking is
      // triggered before reaching the current day timebucket
      (1L to 1000L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        val eventB = PersistentRepr(s"B$n", n, "b", "", writerUuid = w2)
        writeTaggedEvent(t1.plus(n, ChronoUnit.DAYS), eventA, Set("T11"), n, bucketSize)
        writeTaggedEvent(t2.plus(n, ChronoUnit.DAYS), eventB, Set("T11"), n, bucketSize)
      }

      // the search for delayed events should start before we get to the current timebucket
      // until 0.26/0.51 backtracking was broken and events would be skipped
      val src = queries.eventsByTag(tag = "T11", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2000)
      probe.expectNextN(2000)
      probe.cancel()
    }
  }
}

class EventsByTagStrictBySeqNoManyInCurrentTimeBucketSpec extends AbstractEventsByTagSpec(EventsByTagSpec.strictConfig) {

  "Cassandra eventsByTag with many events in current time bucket" must {
    "find all current events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5).minusDays(4)
      val t2 = t1.minusMinutes(1)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      // create two events per day before current day timebucket
      (1L to 3L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        val eventB = PersistentRepr(s"B$n", n, "b", "", writerUuid = w2)
        writeTaggedEvent(t1.plus(n, ChronoUnit.DAYS), eventA, Set("T12"), n, bucketSize)
        writeTaggedEvent(t2.plus(n, ChronoUnit.DAYS), eventB, Set("T12"), n, bucketSize)
      }

      val t3 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(1)
      (4L to 100).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        val eventB = PersistentRepr(s"B$n", n, "b", "", writerUuid = w2)
        writeTaggedEvent(t3.plus(n * 2, ChronoUnit.MILLIS), eventA, Set("T12"), n, bucketSize)
        writeTaggedEvent(t3.plus(n * 2 + 1, ChronoUnit.MILLIS), eventB, Set("T12"), n, bucketSize)
      }

      // the search for delayed events should start before we get to the current timebucket
      // until 0.26/0.51 backtracking was broken and events would be skipped
      val src = queries.currentEventsByTag(tag = "T12", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2000)
      probe.expectNextN(200)
      probe.expectComplete()
    }
  }
}

class EventsByTagStrictBySeqMemoryIssueSpec
  extends AbstractEventsByTagSpec(EventsByTagSpec.strictConfig) {

  "Cassandra eventsByTag with many events" must {
    // would need to send another event or implement a periodic back track for this
    "not use more than then buffer capacity when looking for delayed" ignore {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5).minusDays(4)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val w3 = UUID.randomUUID().toString

      // max-buffer-size = 50
      // create 120 events per day in total, 60 from each one of the two persistenceId
      var lastT = t1
      for {
        day <- 0L to 3L
        n <- 1L to 60L
      } {
        val seqNr = 60 * day + n
        val eventA = PersistentRepr(s"A$seqNr", seqNr, "a", "", writerUuid = w1)
        val eventB = PersistentRepr(s"B$n", seqNr, "b", "", writerUuid = w2)
        val t = t1.plus(day, ChronoUnit.DAYS).plus(2 * n, ChronoUnit.MILLIS)
        writeTaggedEvent(t, eventA, Set("T13"), seqNr, bucketSize)
        writeTaggedEvent(t.plus(1L, ChronoUnit.MILLIS), eventB, Set("T13"), seqNr, bucketSize)
        lastT = t
      }

      val src = queries.eventsByTag(tag = "T13", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      val requested1 = 150L
      probe.request(requested1) // somewhere in day 2
      probe.expectNextN(requested1)
      probe.expectNoMessage(200.millis)
      system.log.debug(s"Part 1 done")

      // beginning of current day
      val requested2 = 120L * 4 - requested1 - 90
      probe.request(requested2)
      probe.expectNextN(requested2)
      probe.expectNoMessage(2.seconds) // enough to trigger delayed backtracking
      system.log.debug(s"Part 2 done")

      // request remaining + 30
      val requested3 = 120L * 4 - requested1 - requested2 + 30
      probe.request(requested3)
      probe.expectNextN(requested3 - 30)
      probe.expectNoMessage(2.seconds)
      system.log.debug(s"Part 3 done")

      // delayed events from another persistenceId
      // earlier timestamp than last retrieved event so these will be found by the "backtracking delayed query"
      (1L to 200L).foreach { n =>
        val eventC = PersistentRepr(s"C$n", n, "c", "", writerUuid = w3)
        writeTaggedEvent(lastT.minus(1, ChronoUnit.SECONDS).plus(n, ChronoUnit.MILLIS), eventC, Set("T13"), n, bucketSize)
      }
      probe.expectNextN(30)
      probe.expectNoMessage(200.millis)
      probe.request(1000)
      probe.expectNextN(200L - 30)

      probe.cancel()
    }

    "not use more than then buffer capacity when looking for missing" ignore {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5).minusDays(1)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val w3 = UUID.randomUUID().toString

      // max-buffer-size = 50
      (1L to 100L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        val t = t1.plus(3 * n, ChronoUnit.MILLIS)
        writeTaggedEvent(t, eventA, Set("T14"), n, bucketSize)
      }

      var missingEvent: PersistentRepr = null
      var missingEventTime: LocalDateTime = null
      (101L to 120L).foreach { n =>
        val t = t1.plus(3 * n, ChronoUnit.MILLIS)
        if (n <= 112) {
          val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
          writeTaggedEvent(t, eventA, Set("T14"), n, bucketSize)
        }

        val eventB = PersistentRepr(s"B$n", n, "b", "", writerUuid = w2)
        val t2 = t.plus(1L, ChronoUnit.MILLIS)
        if (n == 113) {
          missingEvent = eventB
          missingEventTime = t2
        } else
          writeTaggedEvent(t2, eventB, Set("T14"), n - 112, bucketSize)
      }

      val src = queries.eventsByTag(tag = "T14", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      val requested1 = 130L
      probe.request(requested1)
      val expected1 = 100L + 12 * 2
      probe.expectNextN(expected1)
      probe.expectNoMessage(2.seconds)

      system.log.debug("writing missing event, 113, and a bunch of delayed from C")
      (1L to 100L).foreach { n =>
        val eventC = PersistentRepr(s"C$n", n, "c", "", writerUuid = w3)
        val t = t1.plus(3 * n + 2, ChronoUnit.MILLIS)
        writeTaggedEvent(t, eventC, Set("T14"), n, bucketSize)
      }
      writeTaggedEvent(missingEventTime, missingEvent, Set("T14"), 101, bucketSize)
      val expected2 = requested1 - expected1
      probe.expectNextN(expected2)
      probe.expectNoMessage(200.millis)

      probe.request(1000)
      probe.expectNextN(8 + 100 - expected2)
      probe.expectNoMessage(200.millis)

      probe.cancel()
    }

    "find all events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5).minusDays(5)
      val w1 = UUID.randomUUID().toString

      // max-buffer-size = 50
      // start at seqNr 1 here to trigger the backtracking mode
      (101L to 430L).foreach { n =>
        val eventA = PersistentRepr(s"B$n", n, "b", "", writerUuid = w1)
        val t = t1.plus(n, ChronoUnit.MILLIS)
        writeTaggedEvent(t, eventA, Set("T15"), n - 100, bucketSize)
      }

      val src = queries.currentEventsByTag(tag = "T15", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      (1 to 10).foreach { _ =>
        probe.request(30)
        probe.expectNextN(30)
        // reduce downstream demand, which will result in limit < maxBufferSize
        probe.expectNoMessage(waitTime)
      }

      probe.request(100)
      probe.expectNextN(30)
      probe.expectComplete
    }
  }
}

object EventsByTagDisabledSpec {
  class CounterActor(val persistenceId: String) extends PersistentActor {
    var state = 0
    override def receiveRecover = {
      case i: Int =>
        state += i
    }

    override def receiveCommand = {
      case i: Int => persist(i) { i =>
        state += i
        sender() ! state
      }
    }
  }

  def props(pid: String): Props = Props(new CounterActor(pid))
}

class EventsByTagDisabledSpec extends AbstractEventsByTagSpec(EventsByTagSpec.disabledConfig) {

  "Events by tag disabled" must {
    "stop tag_views being created" in {
      session.getCluster.getMetadata.getKeyspace(journalName).getTable("tag_views") shouldEqual null
    }

    "stop tag_progress being created" in {
      session.getCluster.getMetadata.getKeyspace(journalName).getTable("tag_write_progress") shouldEqual null
    }

    "fail current events by tag queries" in {
      val greenSrc = queries.currentEventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(1)
      probe.expectError().getMessage shouldEqual "Events by tag queries are disabled"
    }

    "fail live events by tag queries" in {
      val greenSrc = queries.eventsByTag(tag = "green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(1)
      probe.expectError().getMessage shouldEqual "Events by tag queries are disabled"
    }

    "allow recovery" in {
      val a = system.actorOf(EventsByTagDisabledSpec.props("a"))
      a ! 2
      expectMsg(20.seconds, 2)
      a ! PoisonPill

      val aMk2 = system.actorOf(EventsByTagDisabledSpec.props("a"))
      aMk2 ! 4
      expectMsg(20.seconds, 6)
    }
  }
}
