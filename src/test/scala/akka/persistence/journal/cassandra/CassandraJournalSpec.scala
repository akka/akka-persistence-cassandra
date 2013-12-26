package akka.persistence.journal.cassandra

import akka.actor._
import akka.persistence._
import akka.testkit._

import com.datastax.driver.core.Cluster

import org.scalatest._

object CassandraJournalSpec {
  class ExampleProcessor extends Processor {
    override def processorId = "example-processor"
    def receive = {
      case Persistent(payload, sequenceNr) =>
        sender ! payload
        sender ! sequenceNr
        sender ! recoveryRunning
    }
  }
}

class CassandraJournalSpec extends TestKit(ActorSystem("test")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import CassandraJournalSpec._

  val config = system.settings.config.getConfig("cassandra-journal")

  "A Cassandra journal" should {
    "write and replay messages" in {
      val processor1 = system.actorOf(Props[ExampleProcessor])

      processor1 ! Persistent("a")
      expectMsgAllOf("a", 1L, false)

      val processor2 = system.actorOf(Props[ExampleProcessor])

      processor2 ! Persistent("b")
      expectMsgAllOf("a", 1L, true)
      expectMsgAllOf("b", 2L, false)
    }
  }

  override protected def afterAll(): Unit = {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE ${config.getString("keyspace")}")
    system.shutdown()
  }
}
