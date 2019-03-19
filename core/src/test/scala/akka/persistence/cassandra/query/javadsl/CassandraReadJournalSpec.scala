/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query.javadsl

import akka.persistence.cassandra.query.{ javadsl, scaladsl, TestActor }
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.{ Offset, PersistenceQuery }
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object CassandraReadJournalSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages=off
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-journal.event-adapters {
      test-tagger = akka.persistence.cassandra.query.javadsl.TestTagger
    }
    cassandra-journal.event-adapter-bindings = {
      "java.lang.String" = test-tagger
    }
    """).withFallback(CassandraLifecycle.config)
}

class TestTagger extends WriteEventAdapter {
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any = event match {
    case s: String if s.startsWith("a") => Tagged(event, Set("a"))
    case _                              => event
  }
}

class CassandraReadJournalSpec extends CassandraSpec(CassandraReadJournalSpec.config) {

  lazy val javaQueries = PersistenceQuery(system)
    .getReadJournalFor(classOf[javadsl.CassandraReadJournal], scaladsl.CassandraReadJournal.Identifier)

  "Cassandra Read Journal Java API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(TestActor.props("a"))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = javaQueries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.asScala.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("a").cancel()
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(TestActor.props("b"))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = javaQueries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.asScala.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("b").expectComplete()
    }

    "start eventsByTag query" in {
      val src = javaQueries.eventsByTag("a", Offset.noOffset)
      src.asScala
        .map(_.persistenceId)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectNoMessage(100.millis)
        .cancel()
    }

    "start current eventsByTag query" in {
      val src = javaQueries.currentEventsByTag("a", Offset.noOffset)
      src.asScala.map(_.persistenceId).runWith(TestSink.probe[Any]).request(10).expectNext("a").expectComplete()
    }
  }
}
