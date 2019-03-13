/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.{ActorSystem, PoisonPill}
import akka.persistence.cassandra.TestTaggingActor.{Ack, DoASnapshotPlease, SnapShotAck}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Try

object EventsByTagRecoverySpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val keyspaceName = "EventsByTagRecoverySpec"
  val config = ConfigFactory.parseString(s"""
       |akka {
       |  loglevel = DEBUG
       |  actor.debug.unhandled = on
       |}
       |cassandra-journal {
       |  keyspace = $keyspaceName
       |  log-queries = off
       |  events-by-tag {
       |     max-message-batch-size = 2
       |     bucket-size = "Day"
       |  }
       |}
       |cassandra-snapshot-store.keyspace=$keyspaceName
       |
       |cassandra-query-journal = {
       |   first-time-bucket = "${today.minusMinutes(5).format(query.firstBucketFormatter)}"
       |}
       |
       |akka.actor.serialize-messages=off
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class EventsByTagRecoverySpec extends CassandraSpec(EventsByTagRecoverySpec.config) {

  import EventsByTagRecoverySpec._

  lazy val session = {
    cluster.connect(keyspaceName)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    Try {
      session.close()
      cluster.close()
    }
  }

  val waitTime = 100.milliseconds

  "Events by tag recovery" must {

    "continue tag sequence nrs" in {
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val systemTwo = ActorSystem("s2", system.settings.config)

      try {
        val p1 = systemTwo.actorOf(TestTaggingActor.props("p1", Set("blue")))
        (1 to 4) foreach { i =>
          p1 ! s"e-$i"
          expectMsg(Ack)
        }
        p1 ! PoisonPill
        system.log.info("Starting p1 on other actor system")
        val p1take2 = system.actorOf(TestTaggingActor.props("p1", Set("blue")))
        (5 to 8) foreach { i =>
          p1take2 ! s"e-$i"
          expectMsg(Ack)
        }
        p1take2 ! PoisonPill

        val greenTags = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
        val probe = greenTags.runWith(TestSink.probe[Any](system))
        probe.request(9)
        (1 to 8) foreach { i =>
          val event = s"e-$i"
          system.log.debug("Expecting event {}", event)
          probe.expectNextPF { case EventEnvelope(_, "p1", `i`, `event`) => }
        }
        probe.expectNoMessage(waitTime)

        // Now go back to the original actor system to ensure that previous tag writers don't use a cached value
        // for tag pid sequence nrs
        val p1take3 = systemTwo.actorOf(TestTaggingActor.props("p1", Set("blue")))
        (9 to 12) foreach { i =>
          p1take3 ! s"e-$i"
          expectMsg(Ack)
        }

        val greenTagsTake2 = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
        val probeTake2 = greenTagsTake2.runWith(TestSink.probe[Any](system))
        probeTake2.request(13)
        (1 to 12) foreach { i =>
          val event = s"e-$i"
          system.log.debug("Expecting event {}", event)
          probeTake2.expectNextPF { case EventEnvelope(_, "p1", `i`, `event`) => }
        }
        probeTake2.expectNoMessage(waitTime)
        probeTake2.cancel()
      } finally {
        systemTwo.terminate()
      }
    }

    "recover tag if missing from views table" in {
      val systemTwo = ActorSystem("s2", system.settings.config)
      try {
        val p2 = systemTwo.actorOf(TestTaggingActor.props("p2", Set("red", "orange")))
        (1 to 4) foreach { i =>
          p2 ! s"e-$i"
          expectMsg(Ack)
        }

        Thread.sleep(500)
        systemTwo.terminate().futureValue
        session.execute(s"truncate tag_views")
        session.execute(s"truncate tag_write_progress")

        val tProbe = TestProbe()(system)
        val p2take2 = system.actorOf(TestTaggingActor.props("p2", Set("red", "orange")))
        (5 to 8) foreach { i =>
          p2take2.tell(s"e-$i", tProbe.ref)
          tProbe.expectMsg(Ack)
        }

        val queryJournal =
          PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
        val greenTags = queryJournal.eventsByTag(tag = "red", offset = NoOffset)
        val probe = greenTags.runWith(TestSink.probe[Any](system))
        probe.request(9)
        (1 to 8) foreach { i =>
          val event = s"e-$i"
          system.log.debug("Expecting event {}", event)
          probe.expectNextPF { case EventEnvelope(_, "p2", `i`, `event`) => }
        }
        probe.expectNoMessage(waitTime)
        probe.cancel()
      } finally {
        systemTwo.terminate().futureValue
      }
    }

    "recover tags from before a snapshot" in {
      val systemTwo = ActorSystem("s2", system.settings.config)
      try {
        val p3 = systemTwo.actorOf(TestTaggingActor.props("p3", Set("red", "orange")))
        (1 to 4) foreach { i =>
          p3 ! s"e-$i"
          expectMsg(Ack)
        }
        p3 ! DoASnapshotPlease
        expectMsg(SnapShotAck)
        (5 to 8) foreach { i =>
          p3 ! s"e-$i"
          expectMsg(Ack)
        }

        // Longer than the flush interval of 250ms
        Thread.sleep(500)

        systemTwo.terminate().futureValue
        session.execute(s"truncate tag_views")
        session.execute(s"truncate tag_write_progress")

        val tProbe = TestProbe()(system)
        val p3take2 = system.actorOf(TestTaggingActor.props("p3", Set("red", "orange")))
        (9 to 12) foreach { i =>
          p3take2.tell(s"e-$i", tProbe.ref)
          tProbe.expectMsg(Ack)
        }

        val queryJournal =
          PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
        val greenTags = queryJournal.eventsByTag(tag = "red", offset = NoOffset)
        val probe = greenTags.runWith(TestSink.probe[Any](system))
        probe.request(13)
        (1 to 12) foreach { i =>
          val event = s"e-$i"
          system.log.info("Expecting event {}", event)
          probe.expectNextPF { case EventEnvelope(_, "p3", `i`, `event`) => }
        }
        probe.expectNoMessage(waitTime)
        probe.cancel()
      } finally {
        systemTwo.terminate().futureValue
      }
    }

    "recover if snapshot is for the latest sequence nr" in {
      val p = system.actorOf(TestTaggingActor.props("p4", Set("blue")))
      (1 to 4) foreach { i =>
        p ! s"e-$i"
        expectMsg(Ack)
      }
      p ! DoASnapshotPlease
      expectMsg(SnapShotAck)
      watch(p)
      p ! PoisonPill
      expectTerminated(p)

      // Longer than the flush interval of 250ms
      Thread.sleep(500)

      val systemTwo = ActorSystem("s2", system.settings.config)
      val p2 = systemTwo.actorOf(TestTaggingActor.props("p4", Set("blue")))
      p2 ! "e-5"
      expectMsg(Ack)

      try {
        val queryJournal =
          PersistenceQuery(systemTwo).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
        val blueTags = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
        val probe = blueTags.runWith(TestSink.probe[Any](systemTwo))
        probe.request(6)
        (1 to 5) foreach { i =>
          val event = s"e-$i"
          system.log.info("Expecting event {}", event)
          probe.expectNextPF { case EventEnvelope(_, "p4", `i`, `event`) => }
        }
        probe.expectNoMessage(waitTime)
        probe.cancel()

      } finally {
        systemTwo.terminate()
      }
    }
  }
}
