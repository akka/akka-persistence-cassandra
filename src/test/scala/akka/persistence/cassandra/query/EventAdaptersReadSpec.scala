/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.time.{ ZoneOffset, LocalDate }

import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration._

object EventAdaptersReadSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    cassandra-journal.keyspace=EventAdaptersReadSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15
    cassandra-journal.event-adapters.test = "akka.persistence.cassandra.query.TestEventAdapter"
    cassandra-journal.event-adapter-bindings {
      "java.lang.String" = test
    }
    cassandra-journal.tags {
      red = 1
      yellow = 1
      green = 1
    }
    cassandra-query-journal {
      refresh-interval = 500ms
      max-buffer-size = 50
      first-time-bucket = ${TimeBucket(today.minusDays(5)).key}
      eventual-consistency-delay = 2s
    }
    """).withFallback(CassandraLifecycle.config)
}

class EventAdaptersReadSpec
  extends TestKit(ActorSystem("EventAdaptersReadSpec", EventAdaptersReadSpec.config))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "EventAdaptersReadSpec"

  implicit val mat = ActorMaterializer()(system)

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def setup(persistenceId: String, n: Int, prefix: (Int) => String = _ => ""): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for (i <- 1 to n) {
      val message = s"${prefix(i)}$persistenceId-$i"
      ref ! message
      expectMsg(s"$message-done")
    }

    ref
  }

  def tagged(tags: String*)(f: (Int) => String = _ => ""): Int => String = { i =>
    s"tagged:${tags.mkString(",")}:${f(i)}"
  }

  "Cassandra query EventsByPersistenceId" must {

    "not replay dropped events by the event-adapter" in {

      val ref = setup("a", 6, {
        case x if x % 2 == 0 => "dropped:"
        case _               => ""
      })

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-3")
        .expectNoMsg(500.millis)
        .request(2)
        .expectNext("a-5")
        .expectComplete()
    }

    "replay duplicate events by the event-adapter" in {

      setup("b", 3, {
        case x if x % 2 == 0 => "duplicated:"
        case _               => ""
      })

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("b-1", "b-2", "b-2", "b-3")
        .expectComplete()
    }

    "duplicate events with prefix added by the event-adapter" in {

      setup("c", 1, {
        case _ => "prefixed:foo:"
      })

      val src = queries.currentEventsByPersistenceId("c", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("foo-c-1")
        .expectComplete()
    }

  }

  "Cassandra query EventsByTag" must {

    "not replay events dropped by the event-adapter" in {

      setup("d", 6, tagged("red") {
        case x if x % 2 == 0 => "dropped:"
        case _               => ""
      })

      val src = queries.currentEventsByTag("red", 0L)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("d-1", "d-3", "d-5")
        .expectComplete()
    }

    "replay events duplicated by the event-adapter" in {

      setup("e", 3, tagged("yellow") {
        case x if x % 2 == 0 => "duplicated:"
        case _               => ""
      })

      val src = queries.currentEventsByTag("yellow", 0L)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("e-1", "e-2", "e-2", "e-3")
        .expectComplete()
    }

    "replay events transformed by the event-adapter" in {

      setup("e", 3, tagged("green") {
        case x if x % 2 == 0 => "prefixed:foo:"
        case _               => ""
      })

      val src = queries.currentEventsByTag("green", 0L)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("e-1", "foo-e-2", "e-3")
        .expectComplete()
    }
  }

}
