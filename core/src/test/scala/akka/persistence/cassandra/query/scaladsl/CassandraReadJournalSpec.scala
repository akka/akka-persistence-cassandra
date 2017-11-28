/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query.scaladsl

import scala.concurrent.duration._
import akka.actor.{ ActorSystem, Props }
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpecLike }
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraMetricsRegistry }
import akka.persistence.cassandra.query.TestActor
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.PersistenceQuery
import akka.stream.testkit.scaladsl.TestSink
import akka.persistence.query.NoOffset
import com.datastax.driver.core.Cluster

object CassandraReadJournalSpec {
  val config = ConfigFactory.parseString(
    s"""
    akka.loglevel = INFO
    akka.actor.serialize-messages=off
    cassandra-journal.keyspace=ScaladslCassandraReadJournalSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-journal.event-adapters {
      test-tagger = akka.persistence.cassandra.query.scaladsl.TestTagger
    }
    cassandra-journal.event-adapter-bindings = {
      "java.lang.String" = test-tagger
    }
    cassandra-journal.log-queries = off
    """
  ).withFallback(CassandraLifecycle.config)
}

class TestTagger extends WriteEventAdapter {
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any = event match {
    case s: String if s.startsWith("a") =>
      Tagged(event, Set("a"))
    case _ =>
      event
  }
}

class CassandraReadJournalSpec
  extends TestKit(ActorSystem("ScalaCassandraReadJournalSpec", CassandraReadJournalSpec.config))
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override protected def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .withClusterName("scaladslcassandrareadjournalspec")
      .addContactPoint("localhost")
      .withPort(9042)
      .build()
    try {
      cluster.connect().execute("drop keyspace scaladslcassandrareadjournalspec")
    } finally {
      cluster.close()
    }
  }

  override def systemName: String = "ScalaCassandraReadJournalSpec"

  implicit val mat = ActorMaterializer()(system)

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

    // these tests rely on events written in previous tests
    "start eventsByTag query" in {
      val src = queries.eventsByTag("a", NoOffset)
      src.map(_.persistenceId).runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("a")
        .expectNoMessage(100.millis)
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
