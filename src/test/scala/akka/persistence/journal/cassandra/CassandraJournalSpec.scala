package akka.persistence.journal.cassandra

import akka.actor._
import akka.persistence._
import akka.testkit._

import com.datastax.driver.core.Cluster

import org.scalatest._

object CassandraJournalSpec {
  case class Delete(snr: Long, permanent: Boolean)

  class ProcessorA(override val processorId: String) extends Processor {
    def receive = {
      case Persistent(payload, sequenceNr) =>
        sender ! payload
        sender ! sequenceNr
        sender ! recoveryRunning
      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
    }
  }

  class ProcessorB(override val processorId: String) extends Processor {
    val destination = context.actorOf(Props[Destination])
    val channel = context.actorOf(Channel.props("channel"))

    def receive = {
      case p: Persistent => channel forward Deliver(p, destination)
    }
  }

  class Destination extends Actor {
    def receive = {
      case cp @ ConfirmablePersistent(payload, sequenceNr, _) =>
        sender ! s"${payload}-${sequenceNr}"
        cp.confirm()
    }
  }
}

class CassandraJournalSpec extends TestKit(ActorSystem("test")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import CassandraJournalSpec._

  val config = system.settings.config.getConfig("cassandra-journal")

  "A Cassandra journal" should {
    "write and replay messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      processor1 ! Persistent("a")
      expectMsgAllOf("a", 1L, false)

      val processor2 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      processor2 ! Persistent("b")
      expectMsgAllOf("a", 1L, true)
      expectMsgAllOf("b", 2L, false)
    }
    "not replay messages marked as deleted" in {
      val deleteProbe = TestProbe()
      subscribeToDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p2"))
      processor1 ! Persistent("a")
      processor1 ! Persistent("b")
      expectMsgAllOf("a", 1L, false)
      expectMsgAllOf("b", 2L, false)
      processor1 ! Delete(1L, false)
      awaitDeletion(deleteProbe)

      system.actorOf(Props(classOf[ProcessorA], "p2"))
      expectMsgAllOf("b", 2L, true)
    }
    "not replay permanently deleted messages" in {
      val deleteProbe = TestProbe()
      subscribeToDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p3"))
      processor1 ! Persistent("a")
      processor1 ! Persistent("b")
      expectMsgAllOf("a", 1L, false)
      expectMsgAllOf("b", 2L, false)
      processor1 ! Delete(1L, true)
      awaitDeletion(deleteProbe)

      system.actorOf(Props(classOf[ProcessorA], "p3"))
      expectMsgAllOf("b", 2L, true)
    }
    "write delivery confirmations" in {
      val confirmProbe = TestProbe()
      subscribeToConfirmation(confirmProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorB], "p4"))
      processor1 ! Persistent("a")
      processor1 ! Persistent("b")
      expectMsg("a-1")
      expectMsg("b-2")

      awaitConfirmation(confirmProbe)
      awaitConfirmation(confirmProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorB], "p4"))
      processor2 ! Persistent("c")
      expectMsg("c-3")
    }
  }

  def subscribeToConfirmation(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Confirm])

  def subscribeToDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Delete])

  def awaitConfirmation(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Confirm]

  def awaitDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Delete]

  override protected def afterAll(): Unit = {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE ${config.getString("keyspace")}")
    system.shutdown()
  }
}
