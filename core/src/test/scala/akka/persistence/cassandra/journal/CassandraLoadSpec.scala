/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._

object CassandraLoadSpec {
  val config = ConfigFactory
    .parseString(if (CassandraLifecycle.isExternal) {
      "akka.actor.serialize-messages=off"
    } else {
      s"""
      cassandra-journal.replication-strategy = NetworkTopologyStrategy
      cassandra-journal.data-center-replication-factors = ["dc1:1"]
      akka.actor.serialize-messages=off
     """
    })
    .withFallback(CassandraLifecycle.config)

  trait Measure { this: Actor =>
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L
    var stopSequenceNr = 0L

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender() ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  object Processor {
    def props(persistenceId: String, receiver: Option[ActorRef]): Props =
      Props(new Processor(persistenceId, receiver))
  }

  class Processor(override val persistenceId: String, receiver: Option[ActorRef]) extends PersistentActor with Measure {
    override def receiveRecover: Receive = onEvent

    override def receiveCommand: Receive = {
      case c @ "start"     => onStart(c)
      case c @ "stop"      => onStop(c)
      case payload: String => onCommand(payload)
    }

    def onStart(c: String): Unit = {
      startMeasure()
      sender() ! "started"
    }

    def onStop(c: String): Unit = {
      stopMeasure()
    }

    def onCommand(payload: String): Unit =
      persist(payload)(onEvent)

    def onEvent: Receive = {
      case payload: String =>
        receiver match {
          case None    =>
          case Some(r) => r ! s"$payload-$lastSequenceNr"
        }
    }
  }

  object AsyncProcessor {
    def props(persistenceId: String, receiver: Option[ActorRef]): Props =
      Props(new AsyncProcessor(persistenceId, receiver))
  }

  class AsyncProcessor(persistenceId: String, receiver: Option[ActorRef]) extends Processor(persistenceId, receiver) {
    override def onStart(c: String): Unit = {
      deferAsync(c) { _ =>
        startMeasure()
        sender() ! "started"
      }
    }

    override def onStop(c: String): Unit = {
      deferAsync(c) { _ =>
        stopMeasure()
      }
    }

    override def onCommand(payload: String): Unit =
      persistAsync(payload)(onEvent)

  }

}

class CassandraLoadSpec
    extends CassandraSpec(CassandraLoadSpec.config)
    with ImplicitSender
    with WordSpecLike
    with Matchers {

  import CassandraLoadSpec._

  // use PropertyFileSnitch with cassandra-topology.properties
  override def cassandraConfigResource: String = "test-embedded-cassandra-net.yaml"

  private def testThroughput(processor: ActorRef): Unit = {
    val warmCycles = 100L
    val loadCycles = 2000L

    (1L to warmCycles).foreach { i =>
      processor ! "a"
    }
    processor ! "start"
    expectMsg("started")
    (1L to loadCycles).foreach { i =>
      processor ! "a"
    }
    processor ! "stop"
    expectMsgPF(100.seconds) {
      case throughput: Double => println(f"throughput = $throughput%.2f persistent events per second")
    }
  }

  private def testLoad(processor: ActorRef, startAgain: () => ActorRef): Unit = {
    val cycles = 1000L

    (1L to cycles).foreach { i =>
      processor ! "a"
    }
    (1L to cycles).foreach { i =>
      expectMsg(s"a-$i")
    }

    val processor2 = startAgain()
    (1L to cycles).foreach { i =>
      expectMsg(s"a-$i")
    }

    processor2 ! "b"
    expectMsg(s"b-${cycles + 1L}")
  }

  // increase for serious testing
  private val iterations = 3

  "Untyped PersistentActor with Cassandra journal" should {
    "have some reasonable write throughput for persist" in {
      val processor = system.actorOf(Processor.props("p1", None))
      (1 to iterations).foreach { _ =>
        testThroughput(processor)
      }
    }

    "have some reasonable write throughput for persistAsync" in {
      val processor = system.actorOf(AsyncProcessor.props("p1a", None))
      (1 to iterations).foreach { _ =>
        testThroughput(processor)
      }
    }

    "work properly under load for persist" in {
      val processor = system.actorOf(Processor.props("p2", Some(testActor)))
      testLoad(processor, () => system.actorOf(Processor.props("p2", Some(testActor))))
    }

    "work properly under load for persistAsync" in {
      val processor = system.actorOf(AsyncProcessor.props("p2a", Some(testActor)))
      testLoad(processor, () => system.actorOf(AsyncProcessor.props("p2a", Some(testActor))))
    }
  }
}
