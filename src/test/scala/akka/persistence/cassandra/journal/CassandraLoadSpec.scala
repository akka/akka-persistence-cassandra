package akka.persistence.cassandra.journal

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraCleanup
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraLoadSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
    """.stripMargin)

  trait Measure extends { this: Actor ⇒
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L;
    var stopSequenceNr = 0L;

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  class ProcessorA(override val processorId: String) extends Processor with Measure {
    def receive = {
      case "start" => startMeasure()
      case "stop" => stopMeasure()
      case Persistent(payload: String, sequenceNr) =>
    }
  }

  class ProcessorB(override val processorId: String, failAt: Option[Long]) extends Processor {
    def receive = {
      case Persistent(payload: String, sequenceNr) =>
        failAt.foreach(snr => if (snr == sequenceNr) throw new Exception("boom") with NoStackTrace)
        sender ! s"${payload}-${sequenceNr}"
    }

    override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
      message match {
        case Some(p: Persistent) => deleteMessage(p.sequenceNr)
        case _ =>
      }
      super.preRestart(reason, message)
    }
  }
}

import CassandraLoadSpec._

class CassandraLoadSpec extends TestKit(ActorSystem("test", config)) with ImplicitSender with WordSpecLike with Matchers with CassandraCleanup {
  "A Cassandra journal" should {
    "have some reasonable write throughput" in {
      val warmCycles = 100L
      val loadCycles = 1000L

      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1a"))
      1L to warmCycles foreach { i => processor1 ! Persistent("a") }
      processor1 ! "start"
      1L to loadCycles foreach { i => processor1 ! Persistent("a") }
      processor1 ! "stop"
      expectMsgPF(100 seconds) { case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent commands per second") }
    }
    "work properly under load" in {
      val cycles = 1000L

      val processor1 = system.actorOf(Props(classOf[ProcessorB], "p1b", None))
      1L to cycles foreach { i => processor1 ! Persistent("a") }
      1L to cycles foreach { i => expectMsg(s"a-${i}") }

      val processor2 = system.actorOf(Props(classOf[ProcessorB], "p1b", None))
      1L to cycles foreach { i => expectMsg(s"a-${i}") }

      processor2 ! Persistent("b")
      expectMsg(s"b-${cycles + 1L}")
    }
    "work properly under load and failure conditions" in {
      val cycles = 1000L
      val failAt = 217L

      val processor1 = system.actorOf(Props(classOf[ProcessorB], "p1c", Some(failAt)))
      1L to cycles foreach { i => processor1 ! Persistent("a") }
      1L until (failAt) foreach { i => expectMsg(s"a-${i}") }
      1L to cycles foreach { i => if (i != failAt) expectMsg(s"a-${i}") }

      val processor2 = system.actorOf(Props(classOf[ProcessorB], "p1c", None))
      1L to cycles foreach { i => if (i != failAt) expectMsg(s"a-${i}") }

      processor2 ! Persistent("b")
      expectMsg(s"b-${cycles + 1L}")
    }
  }
}
