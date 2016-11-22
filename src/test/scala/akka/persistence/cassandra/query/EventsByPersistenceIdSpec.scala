/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpecLike }
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

import akka.stream.testkit.scaladsl.TestSink

object EventsByPersistenceIdSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages = on
    akka.actor.serialize-creators = on
    cassandra-journal.keyspace=EventsByPersistenceIdSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15
    """).withFallback(CassandraLifecycle.config)
}

class EventsByPersistenceIdSpec
  extends TestKit(ActorSystem("EventsByPersistenceIdSpec", EventsByPersistenceIdSpec.config))
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
    for (i <- 1 to n) {
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

    "stop if there are no events" in {
      val src = queries.currentEventsByPersistenceId("g", 0L, Long.MaxValue)

      src.runWith(TestSink.probe[Any])
        .request(2)
        .expectComplete()
    }

    "produce correct sequence of sequence numbers and offsets" in {
      val ref = setup("h", 3)

      val src = queries.currentEventsByPersistenceId("h", 0L, Long.MaxValue)
      src.map(x => (x.persistenceId, x.sequenceNr, x.offset)).runWith(TestSink.probe[Any])
        .request(3)
        .expectNext(
          ("h", 1, 1),
          ("h", 2, 2),
          ("h", 3, 3)
        )
        .expectComplete()
    }

    "produce correct sequence of events across multiple partitions" in {
      val ref = setup("i", 20)

      val src = queries.currentEventsByPersistenceId("i", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNextN((1 to 10).map(i => s"i-$i"))
        .expectNoMsg(500.millis)
        .request(10)
        .expectNextN((11 to 20).map(i => s"i-$i"))
        .expectComplete()
    }
  }

  "Cassandra live query EventsByPersistenceId" must {
    "find new events" in {
      val ref = setup("j", 3)
      val src = queries.eventsByPersistenceId("j", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("j-1", "j-2", "j-3")

      ref ! "j-4"
      expectMsg("j-4-done")

      probe.expectNext("j-4")
    }

    "find new events if the stream starts after current latest event" in {
      val ref = setup("k", 4)
      val src = queries.eventsByPersistenceId("k", 5L, Long.MaxValue)
      val probe = src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMsg(1000.millis)

      ref ! "k-5"
      expectMsg("k-5-done")
      ref ! "k-6"
      expectMsg("k-6-done")

      probe.expectNext(5, 6)

      ref ! "k-7"
      expectMsg("k-7-done")

      probe.expectNext(7)
    }

    "find new events up to a sequence number" in {
      val ref = setup("l", 3)
      val src = queries.eventsByPersistenceId("l", 0L, 4L)
      val probe = src.map(_.sequenceNr).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2, 3)

      ref ! "l-4"
      expectMsg("l-4-done")

      probe
        .expectNext(4)
        .expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("m", 3)
      val src = queries.eventsByPersistenceId("m", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("m-1", "m-2")
        .expectNoMsg(100.millis)

      ref ! "m-4"
      expectMsg("m-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("m-3")
        .expectNext("m-4")
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("n", 1000)

      val src = queries.eventsByPersistenceId("n", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("n-1", "n-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("n-3", "n-4", "n-5", "n-6", "n-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("n-8", "n-9", "n-10", "n-11", "n-12")
        .expectNoMsg(1000.millis)
    }

    "not produce anything if there aren't any events" in {
      val ref = setup("o2", 1) // Database init.
      val src = queries.eventsByPersistenceId("o", 0L, Long.MaxValue)

      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNoMsg(1000.millis)
    }

    "not produce anything until there are existing events" in {
      val ref = setup("p2", 1) // Database init.
      val src = queries.eventsByPersistenceId("p", 0L, Long.MaxValue)

      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNoMsg(1000.millis)

      setup("p", 2)

      probe
        .expectNext("p-1", "p-2")
        .expectNoMsg(1000.millis)
    }

    "produce correct sequence of events across multiple partitions" in {
      val ref = setup("q", 15)

      val src = queries.eventsByPersistenceId("q", 0L, Long.MaxValue)

      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(16)
        .expectNextN((1 to 15).map(i => s"q-$i"))
        .expectNoMsg(500.millis)

      for (i <- 16 to 21) {
        ref ! s"q-$i"
        expectMsg(s"q-$i-done")
      }

      probe
        .request(6)
        .expectNextN((16 to 21).map(i => s"q-$i"))

      for (i <- 22 to 35) {
        ref ! s"q-$i"
        expectMsg(s"q-$i-done")
      }

      probe
        .request(10)
        .expectNextN((22 to 31).map(i => s"q-$i"))
    }
  }
}
