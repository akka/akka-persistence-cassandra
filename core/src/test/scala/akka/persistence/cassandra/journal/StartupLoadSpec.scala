/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraSpec
import akka.testkit._

import scala.concurrent.duration._

object StartupLoadSpec {
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

class StartupLoadSpec extends CassandraSpec {

  import StartupLoadSpec._

  // important, since we are testing the initialization
  override def awaitPersistenceInit(): Unit = ()

  "Journal initialization" must {

    "handle many persistent actors starting at the same time" in {
      val N = 500
      for (i <- 1 to 3) {
        val probesAndRefs = (1 to N).map { n =>
          val probe = TestProbe()
          val persistenceId = n.toString
          val r = system.actorOf(Props(classOf[ProcessorA], persistenceId, probe.ref))
          r ! s"a-$i"
          (probe, r)
        }

        probesAndRefs.foreach {
          case (p, r) =>
            if (i == 1 && p == probesAndRefs.head._1)
              p.expectMsg(30.seconds, s"a-$i")
            else
              p.expectMsg(s"a-$i")
            p.expectMsg(i.toLong) // seq number
            p.watch(r)
            r ! PoisonPill
            p.expectTerminated(r)
        }
      }
    }
  }

}
