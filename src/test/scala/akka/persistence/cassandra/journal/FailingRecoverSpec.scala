/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal
/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

import akka.actor.Status.Failure
import akka.persistence.cassandra.testkit.CassandraLauncher

import scala.concurrent.duration._
import akka.actor.{ ActorRef, ActorSystem, Props, _ }
import akka.persistence.{ PersistentActor, _ }
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit.{ ImplicitSender, TestKit, TestProbe, _ }
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.Await

object FailingRecoverSpec {
  val config = ConfigFactory.parseString(
    s"""
       |cassandra-journal.port = ${CassandraLauncher.randomPort}
       |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
       |cassandra-journal.keyspace=StartupLoadSpec
       |cassandra-snapshot-store.keyspace=StartupLoadSpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

  val failingConfig = ConfigFactory.parseString(
    """
      |cassandra-journal.connection.read-timeout=10ms
      |cassandra-snapshot-store.connection.read-timeout=10ms
    """.stripMargin
  ).withFallback(config)

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

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      receiver ! Failure(cause)
    }
  }

}

class FailingRecoverSpec extends TestKit(ActorSystem("FailingRecoverSpec", FailingRecoverSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  import StartupLoadSpec._

  override def systemName: String = "FailingRecoverSpec"

  // important, since we are testing the initialization
  override def awaitPersistenceInit(): Unit = ()

  "Journal initialization" should {

    "handle many persistent actors starting at the same time" in {
      val N = 500

      {

        val probes = (1 to N).map { n =>
          val probe = TestProbe()
          val persistenceId = n.toString
          val r = system.actorOf(Props(classOf[ProcessorA], persistenceId, probe.ref))
          r ! s"a"
          probe
        }

        // wait for all to persist
        probes.foreach { p =>
          p.expectMsgType[String](30.seconds)
        }

        Await.ready(system.terminate(), 10.seconds)
      }

      // now start an actor system with low timeouts
      {
        val system = ActorSystem("FailingRecoverSpec", FailingRecoverSpec.failingConfig)

        for (i <- 1 to 3) {
          val probes = (1 to N).map { n =>
            val probe = TestProbe()(system)
            val persistenceId = n.toString
            val r = system.actorOf(Props(classOf[ProcessorA], persistenceId, probe.ref))
            probe
          }

          // all of them should report failure
          probes.foreach { p =>
            p.expectMsgType[Failure](30.seconds)
          }
        }
      }
    }
  }

}
