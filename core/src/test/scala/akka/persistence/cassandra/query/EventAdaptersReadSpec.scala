/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorRef
import akka.persistence.cassandra.{CassandraLifecycle, CassandraSpec}
import akka.persistence.query.NoOffset
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object EventAdaptersReadSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages=off
    cassandra-journal.keyspace=EventAdaptersReadSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15
    cassandra-journal.event-adapters.test = "akka.persistence.cassandra.query.TestEventAdapter"
    cassandra-journal.event-adapter-bindings {
      "java.lang.String" = test
    }
    cassandra-journal.events-by-tag {
      flush-interval = 0ms
    }
    cassandra-query-journal {
      refresh-interval = 500ms
      max-buffer-size = 50
      first-time-bucket = "${today.minusDays(5).format(firstBucketFormatter)}"
    }
    """).withFallback(CassandraLifecycle.config)
}

class EventAdaptersReadSpec extends CassandraSpec(EventAdaptersReadSpec.config) {

  val waitTime = 25.millis

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
      setup("a", 6, {
        case x if x % 2 == 0 => "dropped:"
        case _ => ""
      })

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-3")
        .expectNoMessage(500.millis)
        .request(2)
        .expectNext("a-5")
        .expectComplete()
    }

    "replay duplicate events by the event-adapter" in {

      setup("b", 3, {
        case x if x % 2 == 0 => "duplicated:"
        case _ => ""
      })

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("b-1", "b-2", "b-2", "b-3")
        .expectComplete()
    }

    "duplicate events with prefix added by the event-adapter" in {

      setup("c", 1, _ => "prefixed:foo:")

      val src = queries.currentEventsByPersistenceId("c", 0L, Long.MaxValue)
      src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("foo-c-1")
        .expectComplete()
    }

  }

  "Cassandra query EventsByTag" must {
    "not replay events dropped by the event-adapter" in {

      setup("d", 6, tagged("red") {
        case x if x % 2 == 0 => "dropped:"
        case _ => ""
      })

      val src = queries.eventsByTag("red", NoOffset)
      val sub = src.map(_.event).runWith(TestSink.probe[Any])
      sub.request(10)
      sub.expectNext("d-1", "d-3", "d-5")
      sub.expectNoMessage(waitTime)
      sub.cancel()
    }

    "replay events duplicated by the event-adapter" in {

      setup("e", 3, tagged("yellow") {
        case x if x % 2 == 0 => "duplicated:"
        case _ => ""
      })

      val src = queries.eventsByTag("yellow", NoOffset)
      val sub = src.map(_.event).runWith(TestSink.probe[Any])

      sub
        .request(10)
        .expectNext("e-1", "e-2", "e-2", "e-3")
        .expectNoMessage(waitTime)
      sub.cancel()
    }

    "replay events transformed by the event-adapter" in {

      setup("e", 3, tagged("green") {
        case x if x % 2 == 0 => "prefixed:foo:"
        case _ => ""
      })

      val src = queries.eventsByTag("green", NoOffset)
      val sub = src.map(_.event).runWith(TestSink.probe[Any])
      sub
        .request(10)
        .expectNext("e-1", "foo-e-2", "e-3")
        .expectNoMessage(waitTime)
      sub.cancel()
    }
  }
}
