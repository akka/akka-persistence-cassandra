/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

object CassandraLoadTypedSpec {

  class Measure {
    private val NanoToSecond = 1000.0 * 1000 * 1000

    private var startTime: Long = 0L
    private var stopTime: Long = 0L

    private var startSequenceNr = 0L
    private var stopSequenceNr = 0L

    def startMeasure(seqNr: Long): Unit = {
      startSequenceNr = seqNr
      startTime = System.nanoTime
    }

    def stopMeasure(seqNr: Long): Double = {
      stopSequenceNr = seqNr
      stopTime = System.nanoTime
      NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime)
    }

  }

  type Command = String
  type Event = String

  class State {
    private var seqNr = 0L

    def increment(): Unit =
      seqNr += 1

    def sequenceNr: Long =
      seqNr
  }

  object Processor {
    def behavior(
        persistenceId: PersistenceId,
        probe: ActorRef[String],
        notifyProbeInEventHandler: Boolean): Behavior[Command] = {

      Behaviors.setup[Command] { _ =>
        val measure = new Measure

        def onStart(state: State): Effect[Event, State] = {
          measure.startMeasure(state.sequenceNr)
          probe ! "started"
          Effect.none
        }

        def onStats(state: State): Effect[Event, State] = {
          val throughput = measure.stopMeasure(state.sequenceNr)
          probe ! f"throughput = $throughput%.2f persistent events per second"
          Effect.none
        }

        def onCommand(cmd: Command): Effect[Event, State] = {
          Effect.persist(cmd)
        }

        EventSourcedBehavior[Command, Event, State](
          persistenceId,
          emptyState = new State,
          commandHandler = { (state, cmd) =>
            cmd match {
              case "start" => onStart(state)
              case "stats" => onStats(state)
              case "stop"  => Effect.stop()
              case _       => onCommand(cmd)
            }
          },
          eventHandler = (state, payload) => {
            state.increment()
            // side effecting in event handler is not recommended, but here testing replay
            if (notifyProbeInEventHandler) {
              probe ! s"$payload-${state.sequenceNr}"
            }
            state
          })

      }
    }
  }

}

class CassandraLoadTypedSpec extends CassandraSpec(dumpRowsOnFailure = false) with AnyWordSpecLike with Matchers {

  import CassandraLoadTypedSpec._

  private val testKit = ActorTestKit("CassandraLoadTypedSpec")

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  private def testThroughput(processor: ActorRef[Command], probe: TestProbe[String]): Unit = {
    val warmCycles = 100L
    val loadCycles = 500L // increase for serious testing

    (1L to warmCycles).foreach { i =>
      processor ! "a"
    }
    processor ! "start"
    probe.expectMessage("started")
    (1L to loadCycles).foreach { i =>
      processor ! "a"
    }

    processor ! "stats"
    // takes a bit longer on c* 2.2
    val throughput = probe.expectMessageType[String](10.seconds)
    println(throughput)
  }

  private def testLoad(
      processor: ActorRef[Command],
      startAgain: () => ActorRef[Command],
      probe: TestProbe[String]): Unit = {
    val cycles = 1000L

    (1L to cycles).foreach { i =>
      processor ! "a"
    }
    (1L to cycles).foreach { i =>
      probe.expectMessage(s"a-$i")
    }

    processor ! "stop"
    probe.expectTerminated(processor)

    val processor2 = startAgain()
    (1L to cycles).foreach { i =>
      probe.expectMessage(s"a-$i")
    }

    processor2 ! "b"
    probe.expectMessage(s"b-${cycles + 1L}")
  }

  // increase for serious testing
  private val iterations = 3

  "Typed EventSourcedBehavior with Cassandra journal" must {
    "have some reasonable write throughput" in {
      val probe = testKit.createTestProbe[String]()
      val processor =
        system.spawnAnonymous(
          Processor.behavior(PersistenceId.ofUniqueId("p1"), probe.ref, notifyProbeInEventHandler = false))
      (1 to iterations).foreach { _ =>
        testThroughput(processor, probe)
      }
    }

    "work properly under load" in {
      val probe = testKit.createTestProbe[String]()
      def spawnProcessor() =
        system.spawnAnonymous(
          Processor.behavior(PersistenceId.ofUniqueId("p2"), probe.ref, notifyProbeInEventHandler = true))
      val processor = spawnProcessor()
      testLoad(processor, () => spawnProcessor(), probe)
    }

  }
}
