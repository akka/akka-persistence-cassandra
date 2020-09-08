/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.Actor
import akka.persistence.CapabilityFlag
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.persistence.JournalProtocol.{ ReplayMessages, WriteMessageFailure, WriteMessages, WriteMessagesFailed }

import scala.concurrent.duration._
import akka.persistence.journal._
import akka.persistence.cassandra.CassandraLifecycle
import akka.stream.alpakka.cassandra.CassandraMetricsRegistry
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object CassandraJournalConfiguration {
  val config = ConfigFactory.parseString(s"""
       akka.persistence.cassandra.journal.keyspace=CassandraJournalSpec
       akka.persistence.cassandra.snapshot.keyspace=CassandraJournalSpecSnapshot
       datastax-java-driver {
         basic.session-name = CassandraJournalSpec
         advanced.metrics {
           session.enabled = [ "bytes-sent", "cql-requests"]
         }
       }  
    """).withFallback(CassandraLifecycle.config)

  lazy val perfConfig =
    ConfigFactory.parseString("""
    akka.actor.serialize-messages=off
    akka.persistence.cassandra.journal.keyspace=CassandraJournalPerfSpec
    akka.persistence.cassandra.snapshot.keyspace=CassandraJournalPerfSpecSnapshot
    """).withFallback(config)

}

// Can't use CassandraSpec so needs to do its own clean up
class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalSpec"

  override def supportsRejectingNonSerializableObjects = false

  "A Cassandra Journal" must {
    "insert Cassandra metrics to Cassandra Metrics Registry" in {
      val registry = CassandraMetricsRegistry(system).getRegistry
      val metricsNames = registry.getNames.toArray.toSet
      // metrics category is the configPath of the plugin + the session-name
      metricsNames should contain("akka.persistence.cassandra.CassandraJournalSpec.bytes-sent")
      metricsNames should contain("akka.persistence.cassandra.CassandraJournalSpec.cql-requests")
    }
    "be able to replay messages after serialization failure" in {
      // there is no chance that a journal could create a data representation for type of event
      val notSerializableEvent = new Object {
        override def toString = "not serializable"
      }
      val msg = PersistentRepr(
        payload = notSerializableEvent,
        sequenceNr = 6,
        persistenceId = pid,
        sender = Actor.noSender,
        writerUuid = writerUuid)

      val probe = TestProbe()

      journal ! WriteMessages(List(AtomicWrite(msg)), probe.ref, actorInstanceId)
      val err = probe.expectMsgPF() {
        case fail: WriteMessagesFailed => fail.cause
      }
      probe.expectMsg(WriteMessageFailure(msg, err, actorInstanceId))

      journal ! ReplayMessages(5, 5, 1, pid, probe.ref)
      probe.expectMsg(replayedMessage(5))
    }
  }
}

class CassandraJournalMetaSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalSpec"

  override def supportsRejectingNonSerializableObjects = false
  protected override def supportsMetadata: CapabilityFlag = true
}

class CassandraJournalPerfSpec
    extends JournalPerfSpec(CassandraJournalConfiguration.perfConfig)
    with CassandraLifecycle {
  override def systemName: String = "CassandraJournalPerfSpec"

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def supportsRejectingNonSerializableObjects = false

}
