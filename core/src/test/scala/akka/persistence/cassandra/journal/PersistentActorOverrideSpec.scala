/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence.PersistentActor
import akka.persistence.RuntimePluginConfig
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object PersistentActorOverrideSpec {
  val config = ConfigFactory.parseString(s"""
      akka.loglevel = DEBUG
      akka.persistence.cassandra.journal.keyspace=PersistentActorOverrideSpec
      akka.persistence.cassandra.snapshot.keyspace=PersistentActorOverrideSpec
      
      plugin2 = $${akka.persistence.cassandra}
      plugin2 {
        journal.table = messages2
      }
      
      plugin3 = $${akka.persistence.cassandra}
      
    """).withFallback(CassandraLifecycle.config)

  object TestActor {
    def props(
        persistenceId: String,
        journalPluginId: String = "",
        snapshotPluginId: String = "",
        journalPluginConfig: Config = ConfigFactory.empty,
        snapshotPluginConfig: Config = ConfigFactory.empty): Props =
      Props(new TestActor(persistenceId, journalPluginId, snapshotPluginId, journalPluginConfig, snapshotPluginConfig))

    sealed trait Command
    final case class Append(s: String) extends Command
    final case object GetState extends Command

  }

  class TestActor(
      override val persistenceId: String,
      override val journalPluginId: String,
      override val snapshotPluginId: String,
      override val journalPluginConfig: Config,
      override val snapshotPluginConfig: Config)
      extends PersistentActor
      with RuntimePluginConfig {

    private var state: String = ""

    def receiveRecover: Receive = {
      case s: String => state = applyEvent(s)
    }

    private def applyEvent(event: String): String =
      state + event

    def receiveCommand: Receive = {
      case TestActor.Append(s) =>
        val event = s
        persist(event) { _ =>
          state = applyEvent(event)
          sender() ! state
        }
      case TestActor.GetState =>
        sender() ! state
    }
  }

}

class PersistentActorOverrideSpec extends CassandraSpec(PersistentActorOverrideSpec.config) {

  import PersistentActorOverrideSpec._

  "PersistentActor" must {

    "use overridden journalPluginId" in {
      val pid = nextId()
      val props = TestActor.props(persistenceId = pid, journalPluginId = "plugin2.journal")
      val ref = system.actorOf(props)
      ref ! TestActor.Append("a")
      expectMsg("a")
      ref ! TestActor.Append("b")
      expectMsg("ab")

      val session = CassandraSessionRegistry(system).sessionFor("plugin2", system.dispatcher)
      session.selectAll("select * from PersistentActorOverrideSpec.messages2").futureValue.size should ===(2)

      system.stop(ref)
      val ref2 = system.actorOf(props)
      ref2 ! TestActor.GetState
      expectMsg("ab")
    }

    "use overridden journalPluginConfig" in {
      val pid = nextId()
      // it's only the `journal` section that can be overridden
      // and it must use a unique configPath (plugin id)
      val journalPluginConfig = ConfigFactory.parseString(s"""
        plugin3.journal {
          table = messages3
        }
        """)
      val props = TestActor.props(
        persistenceId = pid,
        journalPluginId = "plugin3.journal",
        journalPluginConfig = journalPluginConfig)
      val ref = system.actorOf(props)
      ref ! TestActor.Append("c")
      expectMsg("c")

      val session = CassandraSessionRegistry(system).sessionFor("plugin3", system.dispatcher)
      session.selectAll("select * from PersistentActorOverrideSpec.messages3").futureValue.size should ===(1)

      system.stop(ref)
      val ref2 = system.actorOf(props)
      ref2 ! TestActor.GetState
      expectMsg("c")
    }
  }

}
