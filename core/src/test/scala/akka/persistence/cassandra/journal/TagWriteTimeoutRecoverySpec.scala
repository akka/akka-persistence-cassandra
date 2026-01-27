/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.EventEnvelope
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object TagWriteTimeoutRecoverySpec {
  val keyspaceName = "TagWriteTimeoutRecoverySpec"

  // High flush-interval so writes stay buffered when actor terminates
  // This tests that PidTerminated allows pending writes to complete
  val config = ConfigFactory.parseString(s"""
       akka {
         actor.debug.unhandled = on
         loglevel = DEBUG
       }
       akka.persistence.cassandra {
         journal.keyspace = $keyspaceName
         log-queries = off
         events-by-tag {
            max-message-batch-size = 100
            bucket-size = "Day"
            # High flush interval so writes stay in buffer when actor terminates
            flush-interval = 2s
         }
         snapshot.keyspace = $keyspaceName
       }

       akka.actor.serialize-messages = off
    """).withFallback(CassandraLifecycle.config)

  case object Ack
  case object AckFailure
  case class GetState(replyTo: ActorRef)
  case class State(events: List[String])

  def taggingActorProps(pId: String, tags: Set[String], probe: Option[ActorRef] = None): Props =
    Props(new TagWriteTimeoutTestActor(pId, tags, probe))

  /**
   * Test actor that handles persist failures gracefully by acknowledging with AckFailure
   * instead of crashing. This allows us to test the scenario where persists "fail" due to
   * tag write timeout but the events are still in the main journal.
   */
  class TagWriteTimeoutTestActor(val persistenceId: String, tags: Set[String], probe: Option[ActorRef])
      extends PersistentActor {

    private var state: List[String] = Nil

    override def receiveRecover: Receive = {
      case RecoveryCompleted =>
        probe.foreach(_ ! RecoveryCompleted)
      case event: String =>
        state = event :: state
    }

    override def receiveCommand: Receive = {
      case event: String =>
        val replyTo = sender()
        persist(Tagged(event, tags)) { _ =>
          state = event :: state
          replyTo ! Ack
        }
      case GetState(replyTo) =>
        replyTo ! State(state.reverse)
    }

    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      // Don't crash - just ack the failure so test can continue
      // The event IS in the main journal, just the tag write timed out
      sender() ! AckFailure
    }
  }
}

class TagWriteTimeoutRecoverySpec extends CassandraSpec(TagWriteTimeoutRecoverySpec.config) {
  import TagWriteTimeoutRecoverySpec._

  val waitTime = 500.milliseconds

  "Tag write with termination and recovery" must {

    "complete tag writes after actor termination when writes are still buffered" in {
      val pid = "buffered-write-test-1"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      // Create actor and persist some events
      // With high flush-interval, the tag writes stay in the buffer
      val recoveryProbe = TestProbe()
      val p1 = system.actorOf(taggingActorProps(pid, Set("green"), Some(recoveryProbe.ref)))

      val events = (1 to 5).map(i => s"event-$i")
      events.foreach { event =>
        p1 ! event
        expectMsg(Ack)
      }

      // Terminate the actor BEFORE the flush interval
      // The tag writes are still buffered, NOT written to Cassandra yet
      // With PidTerminated (instead of DropState), the writes should complete
      watch(p1)
      p1 ! PoisonPill
      expectTerminated(p1)

      // The tag writes should NOT be dropped - they should eventually complete
      // Wait for the TagWriter flush interval to trigger the write
      Thread.sleep(1000)

      // Force a flush by restarting the actor (recovery will send ResetPersistenceId
      // which should not interfere with the pending writes)
      val recoveryProbe2 = TestProbe()
      val p2 = system.actorOf(taggingActorProps(pid, Set("green"), Some(recoveryProbe2.ref)))
      recoveryProbe2.expectMsg(10.seconds, RecoveryCompleted)

      // Verify all events are recovered (from main journal)
      val stateProbe2 = TestProbe()
      p2 ! GetState(stateProbe2.ref)
      val state2 = stateProbe2.expectMsgType[State]
      state2.events should have size events.size
      state2.events shouldBe events.toList

      watch(p2)
      p2 ! PoisonPill
      expectTerminated(p2)

      // Wait for tag writes to complete (either from original buffered writes or recovery)
      Thread.sleep(2000)

      // Query by tag should return all events with correct sequence numbers
      val greenTags = queryJournal.eventsByTag(tag = "green", offset = NoOffset)
      val probe = greenTags.runWith(TestSink.probe[Any](system))
      probe.request(6)

      events.zipWithIndex.foreach {
        case (event, idx) =>
          val seqNr = idx + 1
          system.log.debug("Expecting event {} with seqNr {}", event, seqNr)
          probe.expectNextPF { case EventEnvelope(_, `pid`, `seqNr`, `event`) => }
      }
      probe.expectNoMessage(waitTime)
      probe.cancel()
    }

    "handle concurrent tag writes from original instance and recovery during failover" in {
      val pid = "failover-test-1"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val systemTwo = ActorSystem("s2", system.settings.config)

      try {
        // Create actor on first system
        val recoveryProbe = TestProbe()
        val p1 = system.actorOf(taggingActorProps(pid, Set("purple"), Some(recoveryProbe.ref)))

        // Persist events - with high flush interval, they stay buffered
        val events = (1 to 4).map(i => s"event-$i")
        events.foreach { event =>
          p1 ! event
          expectMsg(Ack)
        }

        // Terminate the actor while tag writes are still buffered
        watch(p1)
        p1 ! PoisonPill
        expectTerminated(p1)

        // Immediately start actor on second system - simulates failover
        // This will trigger recovery and send tag writes for the same events
        // while the original tag writes from system1 may still be completing
        system.log.info("Starting {} on second actor system while original writes may be in flight", pid)
        val recoveryProbe2 = TestProbe()(systemTwo)
        val p2 = systemTwo.actorOf(taggingActorProps(pid, Set("purple"), Some(recoveryProbe2.ref)))
        recoveryProbe2.expectMsg(20.seconds, RecoveryCompleted)

        // Persist more events on the second system
        val moreEvents = (5 to 8).map(i => s"event-$i")
        moreEvents.foreach { event =>
          p2 ! event
          expectMsg(Ack)
        }

        // Wait for all tag writes to complete
        Thread.sleep(3000)

        val tProbe = TestProbe()(systemTwo)
        p2.tell(PoisonPill, tProbe.ref)
        tProbe.watch(p2)
        tProbe.expectTerminated(p2)

        // Query by tag should return all events in order with correct sequence numbers
        // Both the original writes (from system1) and recovery writes (from system2)
        // should complete idempotently without duplicates
        val allEvents = events ++ moreEvents
        val purpleTags = queryJournal.eventsByTag(tag = "purple", offset = NoOffset)
        val probe = purpleTags.runWith(TestSink.probe[Any](system))
        probe.request(allEvents.size + 1)

        allEvents.zipWithIndex.foreach {
          case (event, idx) =>
            val seqNr = idx + 1
            system.log.info("Expecting event {} with seqNr {}", event, seqNr)
            probe.expectNextPF { case EventEnvelope(_, `pid`, `seqNr`, `event`) => }
        }
        probe.expectNoMessage(waitTime)
        probe.cancel()
      } finally {
        systemTwo.terminate().futureValue
      }
    }
  }

  "Tag write with termination" must {

    "complete tag writes after actor termination even when actor is never restarted" in {
      val pid = "no-restart-test-1"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      // Create actor and persist some events
      // With high flush-interval (2s), the tag writes stay in the buffer
      val recoveryProbe = TestProbe()
      val p1 = system.actorOf(taggingActorProps(pid, Set("yellow"), Some(recoveryProbe.ref)))

      val events = (1 to 5).map(i => s"event-$i")
      events.foreach { event =>
        p1 ! event
        expectMsg(Ack)
      }

      // Terminate the actor BEFORE the flush interval
      // The tag writes are still buffered, NOT written to Cassandra yet
      watch(p1)
      p1 ! PoisonPill
      expectTerminated(p1)

      // Do NOT restart the actor - the tag writes should still complete
      // because PidTerminated allows pending writes to finish

      // Wait for the TagWriter flush interval to trigger the write
      // The flush interval is 2s, so we wait a bit longer
      Thread.sleep(3000)

      // Query by tag should return all events with correct sequence numbers
      // even though the actor was never restarted
      val yellowTags = queryJournal.eventsByTag(tag = "yellow", offset = NoOffset)
      val probe = yellowTags.runWith(TestSink.probe[Any](system))
      probe.request(6)

      events.zipWithIndex.foreach {
        case (event, idx) =>
          val seqNr = idx + 1
          system.log.debug("Expecting event {} with seqNr {}", event, seqNr)
          probe.expectNextPF { case EventEnvelope(_, `pid`, `seqNr`, `event`) => }
      }
      probe.expectNoMessage(waitTime)
      probe.cancel()
    }

    "complete tag writes for multiple persistence ids when actors terminate without restart" in {
      val pid1 = "no-restart-multi-1"
      val pid2 = "no-restart-multi-2"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      // Create two actors with the same tag
      val p1 = system.actorOf(taggingActorProps(pid1, Set("orange")))
      val p2 = system.actorOf(taggingActorProps(pid2, Set("orange")))

      // Persist events from both actors
      val events1 = (1 to 3).map(i => s"p1-event-$i")
      val events2 = (1 to 3).map(i => s"p2-event-$i")

      events1.foreach { event =>
        p1 ! event
        expectMsg(Ack)
      }
      events2.foreach { event =>
        p2 ! event
        expectMsg(Ack)
      }

      // Terminate both actors before flush interval
      watch(p1)
      watch(p2)
      p1 ! PoisonPill
      p2 ! PoisonPill
      expectTerminated(p1)
      expectTerminated(p2)

      // Do NOT restart either actor
      // Wait for tag writes to complete
      Thread.sleep(3000)

      // Query by tag should return all events from both pids
      val orangeTags = queryJournal.eventsByTag(tag = "orange", offset = NoOffset)
      val probe = orangeTags.runWith(TestSink.probe[Any](system))
      probe.request(7)

      // Events may come in any order between pids, but should be ordered within each pid
      val receivedEvents = (1 to 6).map(_ =>
        probe.expectNextPF {
          case EventEnvelope(_, pid, seqNr, event) =>
            (pid, seqNr, event)
        })

      // Verify we got all events from pid1
      val pid1Events = receivedEvents.filter(_._1 == pid1)
      pid1Events.map(_._2) shouldBe Seq(1L, 2L, 3L)
      pid1Events.map(_._3) shouldBe events1

      // Verify we got all events from pid2
      val pid2Events = receivedEvents.filter(_._1 == pid2)
      pid2Events.map(_._2) shouldBe Seq(1L, 2L, 3L)
      pid2Events.map(_._3) shouldBe events2

      probe.expectNoMessage(waitTime)
      probe.cancel()
    }
  }
}
