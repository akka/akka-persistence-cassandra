package akka.persistence.cassandra.query

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

import akka.stream.testkit.scaladsl.TestSink

object EventsByPersistenceIdSpec {
  val config = s"""
    akka.loglevel = INFO
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-journal.target-partition-size = 15
               """
}

class EventsByPersistenceIdSpec
  extends TestKit(ActorSystem("EventsByPersistenceIdSpec", ConfigFactory.parseString(EventsByPersistenceIdSpec.config)))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "EventsByPersistenceIdSpec"

  implicit val mat = ActorMaterializer()(system)

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def setup(persistenceId: String, n: Int): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for(i <- 1 to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Cassandra query EventsByPersistenceId" must {
    "find existing events" in {
      val ref = setup("a", 3)

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-2")
        .expectNoMsg(500.millis)
        .request(2)
        .expectNext("a-3")
        .expectComplete()
    }

    "find existing events from a sequence number" in {
      val ref = setup("b", 10)
      val src = queries.currentEventsByPersistenceId("b", 5L, Long.MaxValue)

      src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(6)
        .expectNext(5, 6, 7, 8, 9, 10)
        .expectComplete()
    }

    "not see any events if the stream starts after current latest event" in {
      val ref = setup("c", 3)
      val src = queries.currentEventsByPersistenceId("c", 5L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectComplete()
    }

    "find existing events up to a sequence number" in {
      val ref = setup("d", 3)
      val src = queries.currentEventsByPersistenceId("d", 0L, 2L)
      src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2)
        .expectComplete()
    }

    "not see new events after demand request" in {
      val ref = setup("e", 3)

      val src = queries.currentEventsByPersistenceId("e", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("e-1", "e-2")
        .expectNoMsg(100.millis)

      ref ! "e-4"
      expectMsg("e-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("e-3")
        .expectComplete() // e-4 not seen
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("f", 1000)

      val src = queries.currentEventsByPersistenceId("f", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("f-1", "f-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("f-3", "f-4", "f-5", "f-6", "f-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("f-8", "f-9", "f-10", "f-11", "f-12")
        .expectNoMsg(1000.millis)
    }
  }

  "Cassandra live query EventsByPersistenceId" must {
    "find new events" in {
      val ref = setup("g", 3)
      val src = queries.eventsByPersistenceId("g", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("g-1", "g-2", "g-3")

      ref ! "g-4"
      expectMsg("g-4-done")

      probe.expectNext("g-4")
    }

    "find new events if the stream starts after current latest event" in {
      val ref = setup("h", 4)
      val src = queries.eventsByPersistenceId("h", 5L, Long.MaxValue)
      val probe = src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMsg(1000.millis)

      ref ! "h-5"
      expectMsg("h-5-done")
      ref ! "h-6"
      expectMsg("h-6-done")

      probe.expectNext(5, 6)

      ref ! "h-7"
      expectMsg("h-7-done")

      probe.expectNext(7)
    }

    "find new events up to a sequence number" in {
      val ref = setup("i", 3)
      val src = queries.eventsByPersistenceId("i", 0L, 4L)
      val probe = src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2, 3)

      ref ! "i-4"
      expectMsg("i-4-done")

      probe.expectNext(4).expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("j", 3)
      val src = queries.eventsByPersistenceId("j", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("j-1", "j-2")
        .expectNoMsg(100.millis)

      ref ! "j-4"
      expectMsg("j-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("j-3")
        .expectNext("j-4")
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("k", 1000)

      val src = queries.eventsByPersistenceId("k", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("k-1", "k-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("k-3", "k-4", "k-5", "k-6", "k-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("k-8", "k-9", "k-10", "k-11", "k-12")
        .expectNoMsg(1000.millis)
    }

    "produce correct sequence of sequence numbers and offsets" in {
      val ref = setup("l", 3)

      val src = queries.currentEventsByPersistenceId("l", 0L, Long.MaxValue)
      src.map(x => (x.persistenceId, x.sequenceNr, x.offset)).runWith(TestSink.probe[Any])
        .request(3)
        .expectNext(
          ("l", 1, 1),
          ("l", 2, 2),
          ("l", 3, 3))
        .expectComplete()
    }

    "produce correct sequence of events across multiple partitions" in {
      val ref = setup("m", 20)

      val src = queries.currentEventsByPersistenceId("m", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"m-$i"))
        .expectNoMsg(500.millis)
        .request(10)
        .expectNextN((11 to 20).map(i => s"m-$i"))
        .expectComplete()
    }
  }
}
