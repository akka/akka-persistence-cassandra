/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.persistence.cassandra.testkit.CassandraLauncher
import java.util.UUID

import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object StartupLoadSpec {
  val config = ConfigFactory.parseString(
    s"""
      |cassandra-journal.keyspace=StartupLoadSpec
      |cassandra-snapshot-store.keyspace=StartupLoadSpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

  class ProcessorA(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = {
      case _ =>
    }

    def receiveCommand: Receive = {
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        saveSnapshot(payload)
    }
  }

}

class StartupLoadSpec extends TestKit(ActorSystem("StartupLoadSpec", StartupLoadSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  import StartupLoadSpec._

  override def systemName: String = "StartupLoadSpec"

  // important, since we are testing the initialization
  override def awaitPersistenceInit(): Unit = ()

  "Journal initialization" should {

    "handle many persistent actors starting at the same time" in {
      val N = 500
      for (i <- 1 to 3) {
        val probes = (1 to N).map { n =>
          val probe = TestProbe()
          val persistenceId = n.toString
          val r = system.actorOf(Props(classOf[ProcessorA], persistenceId, probe.ref))
          r ! s"a-$i"
          probe
        }

        probes.foreach { p =>
          if (i == 1 && p == probes.head)
            p.expectMsg(30.seconds, s"a-$i")
          else
            p.expectMsg(s"a-$i")
          p.expectMsg(i.toLong) // seq number
        }
      }
    }
  }

}
