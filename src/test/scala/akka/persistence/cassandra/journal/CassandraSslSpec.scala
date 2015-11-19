package akka.persistence.cassandra.journal

import akka.persistence.cassandra.testkit.CassandraLauncher
import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraSslSpec {
  val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |akka.test.single-expect-default = 10s
      |cassandra-journal.target-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
      |cassandra-journal.ssl.truststore.path="src/test/resources/security/client_truststore.jks"
      |cassandra-journal.ssl.truststore.password="hbbUtqn3Y1D4Tw"
      |cassandra-journal.ssl.keystore.path="src/test/resources/security/client_keystore.jks"
      |cassandra-journal.ssl.keystore.password="5zsGJ0LxnpozNQ"
      |cassandra-snapshot-store.ssl.truststore.path="src/test/resources/security/client_truststore.jks"
      |cassandra-snapshot-store.ssl.truststore.password="hbbUtqn3Y1D4Tw"
      |cassandra-snapshot-store.ssl.keystore.path="src/test/resources/security/client_keystore.jks"
      |cassandra-snapshot-store.ssl.keystore.password="5zsGJ0LxnpozNQ"
    """.stripMargin)

  class ProcessorA(val persistenceId: String) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        sender ! payload
        sender ! lastSequenceNr
        sender ! recoveryRunning
    }
  }
}

import CassandraSslSpec._

class CassandraSslSpec extends TestKit(ActorSystem("CassandraSslSpec", config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle {

  override val withSsl = true

  override def systemName: String = "CassandraSslSpec"

  "A Cassandra journal" ignore {
    "write messages over SSL" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }
    }
  }
}
