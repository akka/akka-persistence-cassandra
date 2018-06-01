/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorRef
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraPluginConfig, CassandraSpec }
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.datastax.driver.core.Session
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object AllPersistenceIdsSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 10
    cassandra-journal.target-partition-size = 15
    """).withFallback(CassandraLifecycle.config)
}

class AllPersistenceIdsSpec extends CassandraSpec(AllPersistenceIdsSpec.config)
  with BeforeAndAfterEach {

  val cfg = system.settings.config.getConfig("cassandra-journal")
  val pluginConfig = new CassandraPluginConfig(system, cfg)

  var session: Session = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    import system.dispatcher
    session = Await.result(pluginConfig.sessionProvider.connect(), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    Try {
      session.close()
      session.getCluster.close()
    }
    super.afterAll()
  }

  override protected def beforeEach() = {
    super.beforeEach()
    deleteAllEvents()
  }

  def all(): Source[String, NotUsed] = queries.persistenceIds().filterNot(_ == "persistenceInit")

  def current(): Source[String, NotUsed] = queries.currentPersistenceIds().filterNot(_ == "persistenceInit")

  private[this] def deleteAllEvents(): Unit = {
    session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.table}")
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

      val src = current()
      src.runWith(TestSink.probe[Any])
        .request(4)
        .expectNextUnordered("a", "b", "c")
        .expectComplete()
    }

    "deliver persistenceId only once if there are multiple events spanning partitions" in {
      setup("d", 100)

      val src = current()
      src.runWith(TestSink.probe[Any])
        .request(10)
        .expectNext("d")
        .expectComplete()
    }

    "find existing persistence ids in batches if there is more of them than max-result-size-query" in {
      for (i <- 1 to 1000) {
        setup(UUID.randomUUID().toString, 1)
      }

      val src = current()
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

      val src = all()
      val probe = src.runWith(TestSink.probe[Any])
        .request(5)
        .expectNextUnordered("e", "f")

      setup("g", 1)

      probe.expectNext("g")
    }

    "find new events after demand request" in {
      setup("h", 1)
      setup("i", 1)
      val src = all()
      val probe = src.runWith(TestSink.probe[Any])

      probe.request(1)
      probe.expectNext()
      probe.expectNoMessage(100.millis)

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

      val src = all()
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNext()
      probe.expectNext()
      probe.expectNoMessage(1000.millis)

      probe.request(2)
      probe.expectNext()
      probe.expectNext()
      probe.expectNoMessage(1000.millis)
    }

    "deliver persistenceId only once if there are multiple events spanning partitions" in {
      setup("p", 1000)

      val src = all()
      val probe = src.runWith(TestSink.probe[Any])

      probe
        .request(10)
        .expectNext("p")
        .expectNoMessage(1000.millis)

      setup("q", 1000)

      probe
        .request(10)
        .expectNext("q")
        .expectNoMessage(1000.millis)
    }
  }
}
