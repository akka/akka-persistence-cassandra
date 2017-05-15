/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID

import scala.concurrent.duration._
import scala.util.Try
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal.Tagged
import akka.persistence.journal.WriteEventAdapter
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.persistence.query.scaladsl.{ CurrentEventsByTagQuery, EventsByTagQuery }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import akka.stream.ActorMaterializer
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import scala.concurrent.Await
import com.typesafe.config.Config
import akka.persistence.query.TimeBasedUUID

object EventsByTagSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages = on
    akka.actor.serialize-creators = on
    cassandra-journal {
      #target-partition-size = 5
      port = ${CassandraLauncher.randomPort}
      keyspace=EventsByTagSpec
      event-adapters {
        color-tagger  = akka.persistence.cassandra.query.ColorFruitTagger
      }
      event-adapter-bindings = {
        "java.lang.String" = color-tagger
      }
      tags {
        green = 1
        black = 1
        blue = 1
        pink = 1
        yellow = 1
        apple = 2
        banana = 2
        #T1 = 1
        T2 = 2
        T3 = 3
        #T4 = 1
      }
    }
    cassandra-query-journal {
      refresh-interval = 500ms
      max-buffer-size = 50
      first-time-bucket = ${TimeBucket(today.minusDays(5)).key}
      eventual-consistency-delay = 2s
    }
    """).withFallback(CassandraLifecycle.config)

  val strictConfig = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    cassandra-query-journal.delayed-event-timeout = 5s
    cassandra-query-journal.eventual-consistency-delay = 1s
    """).withFallback(config)

  val strictConfigFirstOffset1001DaysAgo = ConfigFactory.parseString(s"""
    cassandra-query-journal.first-time-bucket = ${TimeBucket(today.minusDays(1001)).key}
    """).withFallback(strictConfig)
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

abstract class AbstractEventsByTagSpec(override val systemName: String, config: Config)
  extends TestKit(ActorSystem(systemName, config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  import EventsByTagSpec._

  implicit val mat = ActorMaterializer()(system)

  lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

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
    session.prepare(writeStatements.writeMessage)
  }

  def writeTestEvent(time: LocalDateTime, persistent: PersistentRepr, tags: Set[String]): Unit = {
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)

    val serManifest = serializer match {
      case ser2: SerializerWithStringManifest ⇒
        ser2.manifest(persistent)
      case _ ⇒
        if (serializer.includeManifest) persistent.getClass.getName
        else PersistentRepr.Undefined
    }

    val timestamp = time.toInstant(ZoneOffset.UTC).toEpochMilli

    val bs = preparedWriteMessage.bind()
    bs.setString("persistence_id", persistent.persistenceId)
    bs.setLong("partition_nr", 1L)
    bs.setLong("sequence_nr", persistent.sequenceNr)
    bs.setUUID("timestamp", uuid(timestamp))
    bs.setString("timebucket", TimeBucket(timestamp).key)
    tags.foreach { tag =>
      val tagId = writePluginConfig.tags.getOrElse(tag, 1)
      bs.setString("tag" + tagId, tag)
    }
    bs.setInt("ser_id", serializer.identifier)
    bs.setString("ser_manifest", serManifest)
    bs.setString("event_manifest", persistent.manifest)
    bs.setBytes("event", serialized)
    session.execute(bs)
  }

  def uuid(timestamp: Long): UUID = {
    def makeMsb(time: Long): Long = {
      // copied from UUIDs.makeMsb

      // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
      val uuidEpoch = LocalDateTime.of(1582, 10, 15, 0, 0).atZone(ZoneId.of("GMT-0")).toInstant.toEpochMilli
      val timestamp = (time - uuidEpoch) * 10000

      var msb = 0L
      msb |= (0x00000000ffffffffL & timestamp) << 32
      msb |= (0x0000ffff00000000L & timestamp) >>> 16
      msb |= (0x0fff000000000000L & timestamp) >>> 48
      msb |= 0x0000000000001000L // sets the version to 1.
      msb
    }

    val now = UUIDs.timeBased()
    new UUID(makeMsb(timestamp), now.getLeastSignificantBits)
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    Try(session.getCluster.close())
    super.afterAll()
  }
}

class EventsByTagSpec extends AbstractEventsByTagSpec("EventsByTagSpec", EventsByTagSpec.config) {
  import EventsByTagSpec._

  "Cassandra query currentEventsByTag" must {
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
      probe.expectNoMsg(500.millis)
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
      probe.expectNoMsg(100.millis)

      c ! "a green cucumber"
      expectMsg(s"a green cucumber-done")

      probe.expectNoMsg(100.millis)
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
      bananaTimestamp should be <= (bananaTimestamp)

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
      val t1 = today.minusDays(5).atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T1"))
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T1"))
      val t3 = t1.plusDays(1)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T1"))
      val t4 = t1.plusDays(3)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T1"))

      val src = queries.currentEventsByTag(tag = "T1", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }
      probe.expectNoMsg(500.millis)
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
      probe.expectNoMsg(100.millis)

      d ! "a black dog"
      expectMsg(s"a black dog-done")
      d ! "a black night"
      expectMsg(s"a black night-done")

      probe.expectNextPF { case e @ EventEnvelope(_, "d", 1L, "a black dog") => e }
      probe.expectNoMsg(100.millis)
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
      bananaTimestamp should be <= (bananaTimestamp)

      val greenSrc2 = queries.currentEventsByTag(tag = "green", queries.timeBasedUUIDFrom(bananaTimestamp))
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      if (appleTimestamp == bananaTimestamp)
        probe2.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNextPF { case e @ EventEnvelope(_, "c", 1L, "a green cucumber") => e }
      probe2.expectNoMsg(100.millis)
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
      probe2.expectNoMsg(100.millis)
      probe2.cancel()
    }

    "find new events that spans several time buckets" in {
      val t1 = today.minusDays(5).atStartOfDay.plusHours(13)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T1"))
      val t2 = t1.plusHours(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T1"))

      val src = queries.eventsByTag(tag = "T1", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }

      val t3 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T1"))
      val t4 = LocalDateTime.now(ZoneOffset.UTC)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T1"))

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.cancel()
    }

    "sort events by timestamp" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusSeconds(10)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("p1-e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T2"))
      val t3 = LocalDateTime.now(ZoneOffset.UTC)
      val pr3 = PersistentRepr("p1-e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T2"))

      val src = queries.eventsByTag(tag = "T2", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)

      // simulate async eventually consistent Materialized View update
      // that cause p1-e2 to show up before p2-e1
      Thread.sleep(500)
      val t2 = t3.minus(1, ChronoUnit.MILLIS)
      val pr2 = PersistentRepr("p2-e1", 1L, "p2", "", writerUuid = w2)
      writeTestEvent(t2, pr2, Set("T2"))

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
      probe.expectNoMsg(100.millis)

      for (n <- 101 to 200)
        e ! s"yellow-$n"

      for (n <- 101 to 200) {
        val Expected = s"yellow-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMsg(100.millis)

      probe.request(10)
      probe.expectNoMsg(100.millis)
    }

  }

}

class EventsByTagZeroEventualConsistencyDelaySpec
  extends AbstractEventsByTagSpec(
    "EventsByTagZeroEventualConsistencyDelaySpec",
    ConfigFactory.parseString("cassandra-query-journal.eventual-consistency-delay = 0s")
      .withFallback(EventsByTagSpec.strictConfig)
  ) {
  import EventsByTagSpec._

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

class EventsByTagStrictBySeqNoSpec extends AbstractEventsByTagSpec("EventsByTagStrictBySeqNoSpec", EventsByTagSpec.strictConfig) {
  import EventsByTagSpec._

  "Cassandra live eventsByTag with delayed-event-timeout > 0s" must {

    "detect missing sequence number and wait for it" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T3"))

      val t2 = t1.plusSeconds(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T3"))

      val t4 = t1.plusSeconds(3)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T3"))

      val src = queries.eventsByTag(tag = "T3", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }
      probe.expectNoMsg(500.millis)

      val t3 = t1.plusSeconds(2)
      val pr3 = PersistentRepr("e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T3"))

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "e4") => e }
      probe.cancel()
    }

    "detect missing sequence number and fail after timeout" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T4"))

      val t2 = t1.plusSeconds(1)
      val pr2 = PersistentRepr("e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T4"))

      val t4 = t1.plusSeconds(3)
      val pr4 = PersistentRepr("e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t4, pr4, Set("T4"))

      val src = queries.eventsByTag(tag = "T4", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "e2") => e }
      probe.expectNoMsg(1.seconds)
      probe.expectError().getClass should be(classOf[IllegalStateException])
    }

    "detect missing sequence number and go back to find it" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("p1-e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T5"))

      val t2 = t1.plusSeconds(1)
      val pr2 = PersistentRepr("p1-e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t2, pr2, Set("T5"))

      val t3 = t1.plusSeconds(2)
      val pr3 = PersistentRepr("p2-e1", 1L, "p2", "", writerUuid = w2)
      writeTestEvent(t3, pr3, Set("T5"))

      val t4 = t1.plusSeconds(4)
      val pr4 = PersistentRepr("p2-e2", 2L, "p2", "", writerUuid = w2)
      writeTestEvent(t4, pr4, Set("T5"))

      val src = queries.eventsByTag(tag = "T5", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "p1-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "p1-e2") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 1L, "p2-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 2L, "p2-e2") => e }

      // too early p1-e4
      val t5 = t1.plusSeconds(5)
      val pr5 = PersistentRepr("p1-e4", 4L, "p1", "", writerUuid = w1)
      writeTestEvent(t5, pr5, Set("T5"))
      probe.expectNoMsg(500.millis)

      // the delayed p1-e3, and the timeuuid is before p2-e2
      val t6 = t1.plusSeconds(3)
      val pr6 = PersistentRepr("p1-e3", 3L, "p1", "", writerUuid = w1)
      writeTestEvent(t6, pr6, Set("T5"))

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 3L, "p1-e3") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 4L, "p1-e4") => e }

      val t7 = t1.plusSeconds(7)
      val pr7 = PersistentRepr("p2-e3", 3L, "p2", "", writerUuid = w2)
      writeTestEvent(t7, pr7, Set("T5"))
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 3L, "p2-e3") => e }

      probe.cancel()
    }

    "find delayed events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString
      val pr1 = PersistentRepr("p1-e1", 1L, "p1", "", writerUuid = w1)
      writeTestEvent(t1, pr1, Set("T6"))

      val t2 = t1.plusSeconds(2)
      val pr2 = PersistentRepr("p2-e1", 1L, "p2", "", writerUuid = w2)
      writeTestEvent(t2, pr2, Set("T6"))

      val src = queries.eventsByTag(tag = "T6", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 1L, "p1-e1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "p2", 1L, "p2-e1") => e }

      // delayed, and timestamp is before p2-e1
      val t3 = t1.plusSeconds(1)
      val pr3 = PersistentRepr("p1-e2", 2L, "p1", "", writerUuid = w1)
      writeTestEvent(t3, pr3, Set("T6"))

      probe.expectNextPF { case e @ EventEnvelope(_, "p1", 2L, "p1-e2") => e }

      probe.cancel()
    }

    "find delayed events 2" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val t2 = t1.plusSeconds(1)
      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTestEvent(t2, eventA1, Set("T7"))

      val src = queries.eventsByTag(tag = "T7", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "A1") => e }

      // delayed, timestamp is before A1
      val eventB1 = PersistentRepr("B1", 1L, "b", "", writerUuid = w2)
      writeTestEvent(t1, eventB1, Set("T7"))
      val t3 = t1.plusSeconds(2)
      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTestEvent(t3, eventB2, Set("T7"))

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe.cancel()
    }

    "find delayed events 3" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val eventB0 = PersistentRepr("B0", 1L, "b", "", writerUuid = w2)
      writeTestEvent(t1.minusSeconds(1), eventB0, Set("T8"))

      val t2 = t1.plusSeconds(1)
      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTestEvent(t2, eventA1, Set("T8"))

      val src = queries.eventsByTag(tag = "T8", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B0") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "a", 1L, "A1") => e }

      // delayed, timestamp is before A1
      val eventB1 = PersistentRepr("B1", 2L, "b", "", writerUuid = w2)
      writeTestEvent(t1, eventB1, Set("T8"))
      val t3 = t1.plusSeconds(2)
      val eventB2 = PersistentRepr("B2", 3L, "b", "", writerUuid = w2)
      writeTestEvent(t3, eventB2, Set("T8"))

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B1") => e }
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 3L, "B2") => e }

      probe.cancel()
    }

    "find delayed events from offset" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      val eventA1 = PersistentRepr("A1", 1L, "a", "", writerUuid = w1)
      writeTestEvent(t1.plusSeconds(2), eventA1, Set("T9"))

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
      writeTestEvent(t1.plusSeconds(1), eventB1, Set("T9"))
      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTestEvent(t1.plusSeconds(3), eventB2, Set("T9"))

      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe2.cancel()
    }

    "find delayed events when many other events" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)
      val w1 = UUID.randomUUID().toString
      val w2 = UUID.randomUUID().toString

      (1L to 100L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        writeTestEvent(t1.plus(n, ChronoUnit.MILLIS), eventA, Set("T10"))
      }

      val src = queries.eventsByTag(tag = "T10", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(1000)
      probe.expectNextN(100)

      val t2 = t1.plusSeconds(1)
      (101L to 200L).foreach { n =>
        val eventA = PersistentRepr(s"A$n", n, "a", "", writerUuid = w1)
        writeTestEvent(t2.plus(n, ChronoUnit.MILLIS), eventA, Set("T10"))
      }
      probe.expectNextN(100)

      // delayed, timestamp is before A101 but after A100
      val eventB1 = PersistentRepr("B1", 1L, "b", "", writerUuid = w2)
      writeTestEvent(t2.minus(100, ChronoUnit.MILLIS), eventB1, Set("T10"))

      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "B1") => e }
      probe.expectNoMsg(2.second)

      val eventB2 = PersistentRepr("B2", 2L, "b", "", writerUuid = w2)
      writeTestEvent(t2.plusSeconds(1), eventB2, Set("T10"))
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "B2") => e }

      probe.cancel()
    }

    "find events from many persistenceIds" in {
      val t1 = LocalDateTime.now(ZoneOffset.UTC).minusMinutes(5)

      // limit is 50, so let's use something not divisible by 50
      (1L to 70L).foreach { n =>
        val eventA2 = PersistentRepr(s"A$n-2", sequenceNr = 2, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTestEvent(t1.plus(n, ChronoUnit.MILLIS), eventA2, Set("T11"))
      }

      val src = queries.eventsByTag(tag = "T11", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(1000)
      probe.expectNextN(70)

      (101L to 200L).foreach { n =>
        val eventA2 = PersistentRepr(s"A$n-2", sequenceNr = 2, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTestEvent(t1.plus(n, ChronoUnit.MILLIS), eventA2, Set("T11"))
      }
      probe.expectNextN(100)

      (101L to 200L).foreach { n =>
        val eventA3 = PersistentRepr(s"A$n-3", sequenceNr = 3, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTestEvent(t1.plus(500 + n, ChronoUnit.MILLIS), eventA3, Set("T11"))
      }
      probe.expectNextN(100)

      // those are delayed, timestamp before 101-200 A3
      (1L to 70L).foreach { n =>
        val eventA3 = PersistentRepr(s"A$n-3", sequenceNr = 3, persistenceId = s"a$n", "",
          writerUuid = UUID.randomUUID().toString)
        writeTestEvent(t1.plus(200 + n, ChronoUnit.MILLIS), eventA3, Set("T11"))
      }
      probe.expectNextN(70)

      probe.expectNoMsg(1.second)
      probe.cancel()
    }

  }

}

class EventsByTagStrictBySeqNoEarlyFirstOffsetSpec
  extends AbstractEventsByTagSpec("EventsByTagStrictBySeqNoEarlyFirstOffsetSpec", EventsByTagSpec.strictConfigFirstOffset1001DaysAgo) {
  import EventsByTagSpec._

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
        writeTestEvent(t1.plus(n, ChronoUnit.DAYS), eventA, Set("T11"))
        writeTestEvent(t2.plus(n, ChronoUnit.DAYS), eventB, Set("T11"))
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

