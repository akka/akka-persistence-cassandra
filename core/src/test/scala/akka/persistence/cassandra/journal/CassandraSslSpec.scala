/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.journal.CassandraSslSpec._
import akka.persistence.cassandra.{CassandraLifecycle, CassandraSpec}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import javax.crypto.Cipher
import org.scalatest._

object CassandraSslSpec {
  def config(keyStore: Boolean) = {
    val trustStoreConfig =
      s"""
        |akka.persistence.journal.max-deletion-batch-size = 3
        |akka.persistence.publish-confirmations = on
        |akka.persistence.publish-plugin-commands = on
        |cassandra-journal.target-partition-size = 5
        |cassandra-journal.max-result-size = 3
        |cassandra-journal.keyspace=CassandraSslSpec${if (keyStore) 1 else 2}
        |cassandra-snapshot-store.keyspace=CassandraSslSpec${if (keyStore) 1 else 2}Snapshot
        |cassandra-snapshot-store.ssl.truststore.path="core/src/test/resources/security/cts_truststore.jks"
        |cassandra-snapshot-store.ssl.truststore.password="hbbUtqn3Y1D4Tw"
        |cassandra-journal.ssl.truststore.path="core/src/test/resources/security/cts_truststore.jks"
        |cassandra-journal.ssl.truststore.password="hbbUtqn3Y1D4Tw"
      """.stripMargin

    val keyStoreConfig = if (keyStore) {
      s"""
      |cassandra-snapshot-store.ssl.keystore.path="core/src/test/resources/security/cts_keystore.jks"
      |cassandra-snapshot-store.ssl.keystore.password="5zsGJ0LxnpozNQ"
      |cassandra-journal.ssl.keystore.path="core/src/test/resources/security/cts_keystore.jks"
      |cassandra-journal.ssl.keystore.password="5zsGJ0LxnpozNQ"
      """.stripMargin
    } else ""

    ConfigFactory.parseString(trustStoreConfig + keyStoreConfig).withFallback(CassandraLifecycle.config)
  }

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

trait CassandraSslSpec extends WordSpecLike {

  def hasJCESupport: Boolean =
    Cipher.getMaxAllowedKeyLength("AES") == Int.MaxValue

  def skipIfNoJCESupport(): Unit =
    if (!hasJCESupport) {
      info("Skipping test because Java Cryptography Extensions (JCE) not installed")
      pending
    }

}

class CassandraSslSpecWithClientAuth
    extends TestKit(ActorSystem("CassandraSslSpecWithClientAuth", config(true)))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with CassandraLifecycle
    with CassandraSslSpec {

  override def cassandraConfigResource: String = "test-embedded-cassandra-ssl-server-client.yaml"

  override def systemName: String = "CassandraSslSpec"

  override protected def beforeAll(): Unit =
    if (hasJCESupport)
      super.beforeAll()

  "A Cassandra journal with 2-way SSL setup" must {

    if (CassandraLifecycle.isExternal)
      pending

    "write messages over SSL" in {
      skipIfNoJCESupport()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }
    }
  }

  // Doesn't work in external mode
  override protected def externalCassandraCleanup(): Unit = ()
}

class CassandraSslSpecWithoutClientAuth extends CassandraSpec(config(false)) with CassandraSslSpec {

  override def cassandraConfigResource: String = "test-embedded-cassandra-ssl-server.yaml"

  override protected def beforeAll(): Unit =
    if (hasJCESupport)
      super.beforeAll()

  "A Cassandra journal with 1-way SSL setup" must {

    if (CassandraLifecycle.isExternal)
      pending

    "write messages over SSL" in {
      skipIfNoJCESupport()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }
    }
  }

  // Doesn't work in external mode
  override protected def externalCassandraCleanup(): Unit = ()
}
