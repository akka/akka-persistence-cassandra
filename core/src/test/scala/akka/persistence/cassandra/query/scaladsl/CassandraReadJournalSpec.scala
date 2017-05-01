/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query.scaladsl

import scala.concurrent.duration._
import akka.actor.{ Props, ActorSystem }
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Second, Span }
import org.scalatest.{ Matchers, WordSpecLike }
import akka.persistence.cassandra.{ CassandraMetricsRegistry, CassandraLifecycle }
import akka.persistence.cassandra.query.TestActor
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.PersistenceQuery
import akka.stream.testkit.scaladsl.TestSink
import akka.persistence.query.NoOffset

object CassandraReadJournalSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    cassandra-journal.keyspace=ScaladslCassandraReadJournalSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.eventual-consistency-delay = 1s
    cassandra-journal.event-adapters {
      test-tagger = akka.persistence.cassandra.query.scaladsl.TestTagger
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

class CassandraReadJournalSpec
  extends TestKit(ActorSystem("ScalaCassandraReadJournalSpec", CassandraReadJournalSpec.config))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "ScalaCassandraReadJournalSpec"

  implicit val mat = ActorMaterializer()(system)

  implicit val patience = PatienceConfig(Span(10, Seconds), Span(1, Second))

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  "Cassandra Read Journal Scala API" must {
    "start eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("a")))
      a ! "a-1"
      expectMsg("a-1-done")

      val src = queries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.map(_.persistenceId).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .cancel()
    }

    "start current eventsByPersistenceId query" in {
      val a = system.actorOf(Props(new TestActor("b")))
      a ! "b-1"
      expectMsg("b-1-done")

      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.map(_.persistenceId).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("b")
        .expectComplete()
    }

    "start eventsByTag query" in {
      val src = queries.eventsByTag("a", NoOffset)
      src.map(_.persistenceId).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectNoMsg(100.millis)
        .cancel()
    }

    "start current eventsByTag query" in {
      val src = queries.currentEventsByTag("a", NoOffset)
      src.map(_.persistenceId).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectComplete()
    }

    "insert Cassandra metrics to Cassandra Metrics Registry" in {
      val registry = CassandraMetricsRegistry(system).getRegistry
      val snapshots = registry.getNames.toArray().filter(value => value.toString.startsWith(s"${CassandraReadJournal.Identifier}"))
      snapshots.length should be > 0
    }
  }
}
