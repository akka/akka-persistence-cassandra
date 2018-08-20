/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ExtendedActorSystem, Props }
import akka.persistence.RecoveryCompleted
import akka.persistence.cassandra.EventWithMetaData.UnknownMetaData
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, EventWithMetaData, Persister }
import akka.serialization.BaseSerializer
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object CassandraSerializationSpec {
  val config = ConfigFactory.parseString(
    s"""
       |akka.actor.serialize-messages=false
       |akka.actor.serializers.crap="akka.persistence.cassandra.journal.BrokenDeSerialization"
       |akka.actor.serialization-identifiers."akka.persistence.cassandra.journal.BrokenDeSerialization" = 666
       |akka.actor.serialization-bindings {
       |  "akka.persistence.cassandra.Persister$$CrapEvent" = crap
       |}
       |akka.persistence.journal.max-deletion-batch-size = 3
       |akka.persistence.publish-confirmations = on
       |akka.persistence.publish-plugin-commands = on
       |cassandra-journal.target-partition-size = 5
       |cassandra-journal.max-result-size = 3
       |cassandra-journal.keyspace=CassandraIntegrationSpec
       |cassandra-snapshot-store.keyspace=CassandraIntegrationSpecSnapshot
       |
    """.stripMargin).withFallback(CassandraLifecycle.config)

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

class CassandraSerializationSpec extends CassandraSpec(CassandraSerializationSpec.config) {

  import akka.persistence.cassandra.Persister._

  "A Cassandra journal" should {

    "Fail recovery when deserialization fails" in {
      val probe = TestProbe()
      val incarnation1 = system.actorOf(Props(new Persister("id1", probe.ref)))
      probe.expectMsgType[RecoveryCompleted]

      incarnation1 ! CrapEvent(1)
      probe.expectMsg(CrapEvent(1))

      probe.watch(incarnation1)
      system.stop(incarnation1)
      probe.expectTerminated(incarnation1)

      val incarnation2 = system.actorOf(Props(new Persister("id1", probe.ref)))
      probe.expectMsgType[RuntimeException].getMessage shouldBe "I can't deserialize a single thing"
      incarnation2
    }

    "be able to store meta data" in {
      val probe = TestProbe()
      val incarnation1 = system.actorOf(Props(new Persister("id2", probe.ref)))
      probe.expectMsgType[RecoveryCompleted]

      val eventWithMeta = EventWithMetaData("TheActualEvent", "TheAdditionalMetaData")
      incarnation1 ! eventWithMeta
      probe.expectMsg(eventWithMeta)

      probe.watch(incarnation1)
      system.stop(incarnation1)
      probe.expectTerminated(incarnation1)

      system.actorOf(Props(new Persister("id2", probe.ref)))
      probe.expectMsg(eventWithMeta) // from replay
    }

    "not fail replay due to deserialization problem of meta data" in {
      val probe = TestProbe()
      val incarnation1 = system.actorOf(Props(new Persister("id3", probe.ref)))
      probe.expectMsgType[RecoveryCompleted]

      val eventWithMeta = EventWithMetaData("TheActualEvent", CrapEvent(13))
      incarnation1 ! eventWithMeta
      probe.expectMsg(eventWithMeta)

      probe.watch(incarnation1)
      system.stop(incarnation1)
      probe.expectTerminated(incarnation1)

      system.actorOf(Props(new Persister("id3", probe.ref)))
      probe.expectMsg(EventWithMetaData("TheActualEvent", UnknownMetaData(666, ""))) // from replay, no meta
    }

  }

}
