/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, ActorSystem, ExtendedActorSystem, Props }
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.serialization.{ BaseSerializer, Serializer }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpecLike }

object CassandraSerializationSpec {
  val config = ConfigFactory.parseString(
    s"""
       |akka.actor.serialize-messages=false
       |akka.actor.serializers.crap="akka.persistence.cassandra.journal.BrokenDeSerialization"
       |akka.actor.serialization-identifiers."akka.persistence.cassandra.journal.BrokenDeSerialization" = 666
       |akka.actor.serialization-bindings {
       |  "akka.persistence.cassandra.journal.CassandraSerializationSpec$$PersistsEverythingButCantKeepAnythingToHimself$$Event" = crap
       |}
       |akka.persistence.journal.max-deletion-batch-size = 3
       |akka.persistence.publish-confirmations = on
       |akka.persistence.publish-plugin-commands = on
       |cassandra-journal.target-partition-size = 5
       |cassandra-journal.max-result-size = 3
       |cassandra-journal.keyspace=CassandraIntegrationSpec
       |cassandra-snapshot-store.keyspace=CassandraIntegrationSpecSnapshot
       |
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

  object PersistsEverythingButCantKeepAnythingToHimself {
    case class Event(n: Int)
  }

  class PersistsEverythingButCantKeepAnythingToHimself(override val persistenceId: String, probe: ActorRef) extends PersistentActor {
    override def receiveRecover: Receive = {
      case msg => probe ! msg
    }
    override def receiveCommand: Receive = {
      case msg => persist(msg) { persisted =>
        probe ! msg
      }
    }

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      probe ! cause
    }

  }
}

class BrokenDeSerialization(override val system: ExtendedActorSystem) extends BaseSerializer {
  override def includeManifest: Boolean = false
  override def toBinary(o: AnyRef): Array[Byte] = {
    // I was serious with the class name
    Array.emptyByteArray
  }
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    throw new RuntimeException("I can't deserialize a single thing")
  }
}

class CassandraSerializationSpec extends TestKit(ActorSystem("CassandraSerializationSpec", CassandraSerializationSpec.config)) with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  override def systemName: String = "CassandraSerializationSpec"

  import CassandraSerializationSpec._
  import PersistsEverythingButCantKeepAnythingToHimself._

  "A Cassandra journal" should {

    "Fail recovery when deserialization fails" in {
      val probe = TestProbe()
      val incarnation1 = system.actorOf(Props(new PersistsEverythingButCantKeepAnythingToHimself("id1", probe.ref)))
      probe.expectMsgType[RecoveryCompleted]

      incarnation1 ! Event(1)
      probe.expectMsg(Event(1))

      probe.watch(incarnation1)
      system.stop(incarnation1)
      probe.expectTerminated(incarnation1)

      val incarnation2 = system.actorOf(Props(new PersistsEverythingButCantKeepAnythingToHimself("id1", probe.ref)))
      probe.expectMsgType[RuntimeException].getMessage shouldBe "I can't deserialize a single thing"

    }

  }

}
