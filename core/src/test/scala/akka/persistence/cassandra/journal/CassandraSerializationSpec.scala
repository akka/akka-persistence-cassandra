/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ExtendedActorSystem, Props }
import akka.persistence.RecoveryCompleted
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, Persister }
import akka.serialization.BaseSerializer
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object CassandraSerializationSpec {
  val config = ConfigFactory.parseString(s"""
       |akka.actor.serialize-messages=false
       |akka.actor.serializers.crap="akka.persistence.cassandra.journal.BrokenDeSerialization"
       |akka.actor.serialization-identifiers."akka.persistence.cassandra.journal.BrokenDeSerialization" = 666
       |akka.actor.serialization-bindings {
       |  "akka.persistence.cassandra.Persister$$CrapEvent" = crap
       |}
       |akka.persistence.journal.max-deletion-batch-size = 3
       |akka.persistence.publish-confirmations = on
       |akka.persistence.publish-plugin-commands = on
       |akka.persistence.cassandra.journal.target-partition-size = 5
       |akka.persistence.cassandra.max-result-size = 3
       |akka.persistence.cassandra.journal.keyspace=CassandraIntegrationSpec
       |akka.persistence.cassandra.snapshot.keyspace=CassandraIntegrationSpecSnapshot
       |
    """.stripMargin).withFallback(CassandraLifecycle.config)

}

class BrokenDeSerialization(override val system: ExtendedActorSystem) extends BaseSerializer {
  override def includeManifest: Boolean = false
  override def toBinary(o: AnyRef): Array[Byte] =
    // I was serious with the class name
    Array.emptyByteArray
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
    throw new RuntimeException("I can't deserialize a single thing")
}

class CassandraSerializationSpec extends CassandraSpec(CassandraSerializationSpec.config) {

  import akka.persistence.cassandra.Persister._

  "A Cassandra journal" must {

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

  }

}
