/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.journal.JournalSettings
import akka.persistence.query.AllEventsOffset
import akka.persistence.query.EventEnvelope
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

object AllEventsSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.remote.artery.canonical.hostname = "127.0.0.1"
    akka.cluster.pub-sub.send-to-dead-letters-when-no-subscribers = off
    akka.persistence.max-concurrent-recoveries = 50
    akka.persistence.cassandra {
      journal.target-partition-size = 15
      query {
        max-buffer-size = 10
        refresh-interval = 0.5s
        max-result-size-query = 10
      }
      events-by-tag.enabled = off
    }  
    """).withFallback(CassandraLifecycle.config)
}

class AllEventsSpec extends CassandraSpec(AllEventsSpec.config) with BeforeAndAfterEach {

  val cfg = system.settings.config.getConfig("akka.persistence.cassandra")
  val journalSettings = new JournalSettings(system, cfg)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    deleteAllEvents()
  }

  private def deleteAllEvents(): Unit = {
    cluster.execute(s"TRUNCATE ${journalSettings.keyspace}.${journalSettings.table}")
    cluster.execute(s"TRUNCATE ${journalSettings.keyspace}.${journalSettings.allPersistenceIdsTable}")
  }

  private def setup(persistenceId: String, n: Int, await: Boolean = true): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    val replyTo = if (await) testActor else system.deadLetters
    for (i <- 1 to n) {
      ref.tell(s"$persistenceId-$i", replyTo)
      if (await)
        expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Cassandra query allEvents" must {
    "find existing events" in {
      setup("a", 1)
      setup("b", 3)
      setup("c", 2)

      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset.empty, _ => ())
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(100)
      val received = probe.expectNextN(6)

      received.filter(_.persistenceId == "a").map(_.event) should ===(Vector("a-1"))
      received.filter(_.persistenceId == "b").map(_.event) should ===(Vector("b-1", "b-2", "b-3"))
      received.filter(_.persistenceId == "c").map(_.event) should ===(Vector("c-1", "c-2"))

      probe.cancel()
    }

    "find new events" in {
      val refA = setup("a", 1)

      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset.empty, _ => ())
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(100)

      probe.expectNext().event should ===("a-1")
      refA ! "a-2"
      expectMsg("a-2-done")
      refA ! "a-3"
      expectMsg("a-3-done")
      probe.expectNext().event should ===("a-2")
      probe.expectNext().event should ===("a-3")

      val refB = setup("b", 3)
      probe.expectNext().event should ===("b-1")
      probe.expectNext().event should ===("b-2")
      probe.expectNext().event should ===("b-3")

      refB ! "b-4"
      expectMsg("b-4-done")
      probe.expectNext().event should ===("b-4")

      refA ! "a-4"
      expectMsg("a-4-done")
      probe.expectNext().event should ===("a-4")

      probe.cancel()
    }

    "resume from offset" in {
      setup("a", 2)
      setup("b", 4)

      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset(Map("a" -> 1L, "b" -> 2)), _ => ())
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      var received = Vector.empty[EventEnvelope]
      probe.request(100)
      received ++= probe.expectNextN(3)
      probe.expectNoMessage()

      received.filter(_.persistenceId == "a").map(_.event) should ===(Vector("a-2"))
      received.filter(_.persistenceId == "b").map(_.event) should ===(Vector("b-3", "b-4"))

      probe.cancel()
    }

    "deliver direct events without query" in {
      val directRef = new AtomicReference[ActorRef]
      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset.empty, ref => directRef.set(ref))
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(100)

      awaitCond(directRef.get != null)

      val refA = setup("a", 2)
      probe.expectNext().event should ===("a-1")
      probe.expectNext().event should ===("a-2")
      // might take some longer for PersistenceIdsState actor to see the update
      probe.expectNoMessage(500.millis)

      directRef.get ! PersistentRepr("a-3", 3, "a")
      probe.expectNext().event should ===("a-3")
      directRef.get ! PersistentRepr("a-4", 4, "a")
      probe.expectNext().event should ===("a-4")

      // missing a-5
      directRef.get ! PersistentRepr("a-6", 6, "a")
      probe.expectNoMessage()
      refA ! "a-3"
      refA ! "a-4"
      refA ! "a-5"
      refA ! "a-6"
      expectMsg("a-3-done")
      expectMsg("a-4-done")
      expectMsg("a-5-done")
      expectMsg("a-6-done")
      probe.expectNext().event should ===("a-5")
      probe.expectNext().event should ===("a-6")

      // might take some longer for PersistenceIdsState actor to see the update
      probe.expectNoMessage(500.millis)
      directRef.get ! PersistentRepr("a-7", 7, "a")
      directRef.get ! PersistentRepr("a-8", 8, "a")
      probe.expectNext().event should ===("a-7")
      probe.expectNext().event should ===("a-8")

      probe.cancel()
    }

    "deliver direct events when first seqNr is expected" in {
      val directRef = new AtomicReference[ActorRef]
      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset(Map("c" -> 3)), ref => directRef.set(ref))
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(100)

      awaitCond(directRef.get != null)

      directRef.get ! PersistentRepr("a-1", 1, "a")
      probe.expectNext().event should ===("a-1")
      directRef.get ! PersistentRepr("a-2", 2, "a")
      probe.expectNext().event should ===("a-2")
      directRef.get ! PersistentRepr("a-3", 3, "a")
      directRef.get ! PersistentRepr("a-4", 4, "a")
      directRef.get ! PersistentRepr("a-5", 5, "a")

      probe.expectNext().event should ===("a-3")
      probe.expectNext().event should ===("a-4")
      probe.expectNext().event should ===("a-5")

      // new pid must start at 1
      directRef.get ! PersistentRepr("b-2", 2, "b")
      probe.expectNoMessage()
      directRef.get ! PersistentRepr("b-1", 1, "b")
      probe.expectNext().event should ===("b-1")

      // or new pid must start from offset
      directRef.get ! PersistentRepr("c-5", 5, "c")
      probe.expectNoMessage()
      directRef.get ! PersistentRepr("c-4", 4, "c")
      probe.expectNext().event should ===("c-4")

      probe.cancel()
    }

    "handle many persistence ids, without pubsub" in {
      val N1 = 100
      val N2 = 2000

      (1 until N1).foreach { n =>
        setup(s"pid-$n", 3, await = false)
      }
      within(5.seconds + (3.millis * N1)) {
        setup(s"pid-$N1", 3, await = true)
      }

      val src = AllEvents.allEvents(queries, _ => true, AllEventsOffset.empty, _ => ())
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(100000)

      ((N1 + 1) to (N1 + N2)).foreach { n =>
        setup(s"pid-$n", 3, await = false)
      }

      val received =
        probe.within(5.seconds + (3.millis * N2)) {
          probe.expectNextN((N1 + N2) * 3)
        }

      received.filter(_.persistenceId == "pid-1").map(_.event) should ===(Vector("pid-1-1", "pid-1-2", "pid-1-3"))
      received.filter(_.persistenceId == s"pid-$N1").map(_.event) should ===(
        Vector(s"pid-$N1-1", s"pid-$N1-2", s"pid-$N1-3"))
      received.filter(_.persistenceId == s"pid-${N1 + N2}").map(_.event) should ===(
        Vector(s"pid-${N1 + N2}-1", s"pid-${N1 + N2}-2", s"pid-${N1 + N2}-3"))

      probe.cancel()
    }

    "handle many persistence ids, with pubsub" in {
      val N1 = 100
      val N2 = 2000

      (1 until N1).foreach { n =>
        setup(s"TestEntity|pid-$n", 3, await = false)
      }
      within(5.seconds + (3.millis * N1)) {
        setup(s"TestEntity|pid-$N1", 3, await = true)
      }

      val src = AllEvents.allEvents(system, queries, AllEventsOffset.empty, "TestEntity", 0, 1)
      val probe = src.runWith(TestSink.probe[EventEnvelope])
      probe.request(1000000)

      ((N1 + 1) to (N1 + N2)).foreach { n =>
        setup(s"TestEntity|pid-$n", 3, await = false)
      }

      val received =
        probe.within(5.seconds + (3.millis * N2)) {
          probe.expectNextN((N1 + N2) * 3)
        }

      received.filter(_.persistenceId == "TestEntity|pid-1").map(_.event) should ===(
        Vector("TestEntity|pid-1-1", "TestEntity|pid-1-2", "TestEntity|pid-1-3"))
      received.filter(_.persistenceId == s"TestEntity|pid-$N1").map(_.event) should ===(
        Vector(s"TestEntity|pid-$N1-1", s"TestEntity|pid-$N1-2", s"TestEntity|pid-$N1-3"))
      received.filter(_.persistenceId == s"TestEntity|pid-${N1 + N2}").map(_.event) should ===(
        Vector(s"TestEntity|pid-${N1 + N2}-1", s"TestEntity|pid-${N1 + N2}-2", s"TestEntity|pid-${N1 + N2}-3"))

      probe.cancel()
    }

  }
}
