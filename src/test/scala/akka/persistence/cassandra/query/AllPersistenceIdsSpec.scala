/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.util.UUID

import scala.concurrent.duration._

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpecLike }
import org.scalatest.concurrent.ScalaFutures

import akka.persistence.cassandra.{ CassandraPluginConfig, CassandraLifecycle }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.PersistenceQuery

import scala.util.Try

object AllPersistenceIdsSpec {
  val config = s"""
    akka.loglevel = INFO
    akka.test.single-expect-default = 10s
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 10
    cassandra-journal.target-partition-size = 15
               """
}

class AllPersistenceIdsSpec
  extends TestKit(ActorSystem("AllPersistenceIdsSpec", ConfigFactory.parseString(AllPersistenceIdsSpec.config)))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers
  with BeforeAndAfterEach {

  override def systemName: String = "AllPersistenceIdsSpec"

  val cfg = ConfigFactory.parseString(AllPersistenceIdsSpec.config)
    .withFallback(system.settings.config)
    .getConfig("cassandra-journal")
  val pluginConfig = new CassandraPluginConfig(cfg)

  override protected def beforeEach() = {
    super.beforeEach()
    deleteAllEvents()
  }

  implicit val mat = ActorMaterializer()(system)

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  private[this] def deleteAllEvents(): Unit = {
    val session = pluginConfig.clusterBuilder.build.connect()
    Try(session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.table}"))
  }

  private[this] def setup(persistenceId: String, n: Int): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for (i <- 1 to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Cassandra query CurrentPersistenceIds" must {
    "find existing events" in {
      setup("a", 1)
      setup("b", 1)
      setup("c", 1)

      val src = queries.currentPersistenceIds()
      src.runWith(TestSink.probe[Any])
        .request(4)
        .expectNextUnordered("a", "b", "c")
        .expectComplete()
    }

    "deliver persistenceId only once if there are multiple events spanning partitions" in {
      setup("d", 100)

      val src = queries.currentPersistenceIds()
      src.runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("d")
        .expectComplete()
    }

    "find existing persistence ids in batches if there is more of them than max-result-size-query" in {
      for (i <- 1 to 1000) {
        setup(UUID.randomUUID().toString, 1)
      }

      val src = queries.currentPersistenceIds()
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(1000)

      for (i <- 1 to 1000) {
        probe.expectNext()
      }

      probe.expectComplete()
    }
  }

  "Cassandra query AllPersistenceIds" must {
    "find new events" in {
      setup("e", 1)
      setup("f", 1)

      val src = queries.allPersistenceIds()
      val probe = src.runWith(TestSink.probe[Any])
        .request(5)
        .expectNextUnordered("e", "f")

      setup("g", 1)

      probe.expectNext("g")
    }

    "find new events after demand request" in {
      setup("h", 1)
      setup("i", 1)
      val src = queries.allPersistenceIds()
      val probe = src.runWith(TestSink.probe[Any])

      probe.request(1)
      probe.expectNext()
      probe.expectNoMsg(100.millis)

      setup("j", 1)

      probe.request(5)
      probe.expectNext()
      probe.expectNext()
    }

    "only deliver what requested if there is more in the buffer" in {
      setup("k", 1)
      setup("l", 1)
      setup("m", 1)
      setup("n", 1)
      setup("o", 1)

      val src = queries.allPersistenceIds()
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNext()
      probe.expectNext()
      probe.expectNoMsg(1000.millis)

      probe.request(2)
      probe.expectNext()
      probe.expectNext()
      probe.expectNoMsg(1000.millis)
    }

    "deliver persistenceId only once if there are multiple events spanning partitions" in {
      setup("p", 1000)

      val src = queries.allPersistenceIds()
      val probe = src.runWith(TestSink.probe[Any])

      probe
        .request(10)
        .expectNext("p")
        .expectNoMsg(1000.millis)

      setup("q", 1000)

      probe
        .request(10)
        .expectNext("q")
        .expectNoMsg(1000.millis)
    }
  }
}
