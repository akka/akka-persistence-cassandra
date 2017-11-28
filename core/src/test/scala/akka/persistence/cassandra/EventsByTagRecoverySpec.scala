/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.{ LocalDateTime, ZoneOffset }

import akka.actor.{ ActorSystem, PoisonPill }
import akka.persistence.cassandra.TestTaggingActor.{ Ack, DoASnapshotPlease, SnapShotAck }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration._

object EventsByTagRecoverySpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val keyspaceName = "EventsByTagRecovery"

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = DEBUG
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
       |   first-time-bucket = "${today.minusMinutes(5).format(query.firstBucketFormat)}"
       |}
       |akka.actor.serialize-messages=off
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)
}

class EventsByTagRecoverySpec extends TestKit(ActorSystem("EventsByTagRecoverySpec", EventsByTagRecoverySpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  import EventsByTagRecoverySpec._

  lazy val session = {
    cluster.connect(keyspaceName)
  }

  override protected def afterAll(): Unit = {
    session.close()
    cluster.close()
    super.afterAll()
  }

  override protected def externalCassandraCleanup(): Unit = {
    cluster.connect().execute(s"drop keyspace $keyspaceName")
    cluster.close()
  }

  override def systemName = keyspaceName
  implicit val materialiser = ActorMaterializer()(system)

  val waitTime = 100.milliseconds

  "Events by tag recovery" must {
    "continue tag sequence nrs" in {
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val systemTwo = ActorSystem("s2", EventsByTagRecoverySpec.config)

      try {
        val p1 = systemTwo.actorOf(TestTaggingActor.props("p1", Set("blue")))
        (1 to 4) foreach { i =>
          p1 ! s"e-$i"
          expectMsg(Ack)
        }
        p1 ! PoisonPill

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
      val systemTwo = ActorSystem("s2", EventsByTagRecoverySpec.config)
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

        implicit val materialiser = ActorMaterializer()(system)
        val tProbe = TestProbe()(system)
        val p2take2 = system.actorOf(TestTaggingActor.props("p2", Set("red", "orange")))
        (5 to 8) foreach { i =>
          p2take2.tell(s"e-$i", tProbe.ref)
          tProbe.expectMsg(Ack)
        }

        val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
        val greenTags = queryJournal.currentEventsByTag(tag = "red", offset = NoOffset)
        val probe = greenTags.runWith(TestSink.probe[Any](system))
        probe.request(9)
        (1 to 8) foreach { i =>
          val event = s"e-$i"
          system.log.info("Expecting event {}", event)
          probe.expectNextPF { case EventEnvelope(_, "p2", `i`, `event`) => }
        }
        probe.expectComplete()
      } finally {
        systemTwo.terminate().futureValue
      }
    }

    "recover tags from before a snapshot" in {
      val systemTwo = ActorSystem("s2", EventsByTagRecoverySpec.config)
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

        implicit val materialiser = ActorMaterializer()(system)
        val tProbe = TestProbe()(system)
        val p3take2 = system.actorOf(TestTaggingActor.props("p3", Set("red", "orange")))
        (9 to 12) foreach { i =>
          p3take2.tell(s"e-$i", tProbe.ref)
          tProbe.expectMsg(Ack)
        }

        val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
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
  }
}
