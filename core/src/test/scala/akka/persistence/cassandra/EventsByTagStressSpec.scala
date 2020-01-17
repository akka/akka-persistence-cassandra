/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.persistence.cassandra.query.TestActor
import akka.persistence.cassandra.query._
import akka.persistence.journal.Tagged
import akka.persistence.query.NoOffset
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

import scala.collection.immutable
import scala.concurrent.Future

class EventsByTagStressSpec extends CassandraSpec(s"""
    cassandra-plugin {
      events-by-tag {
        max-message-batch-size = 25
      }
    }
  """) {

  implicit val ec = system.dispatcher

  val writers = 10
  val readers = 20
  val messages = 5000

  "EventsByTag" must {

    "work under load" in {
      val pas = (0 until writers).map { i =>
        system.actorOf(TestActor.props(s"pid$i"))
      }

      val eventsByTagQueries: immutable.Seq[(Int, TestSubscriber.Probe[(String, Int)])] = (0 until readers).map { i =>
        val probe = queryJournal
          .eventsByTag("all", NoOffset)
          .map(i => {
            (i.persistenceId, i.event.asInstanceOf[Int])
          })
          .runWith(TestSink.probe)
        (i, probe)
      }

      system.log.info("Started events by tag queries")

      val writes: Future[Unit] = Future {
        system.log.info("Sending messages")
        (0 until messages).foreach { i =>
          pas.foreach(ref => {
            ref ! Tagged(i, Set("all"))
            expectMsg(s"$i-done")
          })
        }
        system.log.info("Sent messages")
      }
      writes.onComplete(result => system.log.info("{}", result))

      system.log.info("Reading messages")
      var latestValues: Map[(Int, String), Int] = Map.empty.withDefault(_ => -1)
      (0 until messages).foreach { _ =>
        (0 until writers).foreach { _ =>
          eventsByTagQueries.foreach {
            case (probeNr, probe) =>
              // should be in order per persistence id per probe
              val (pid, msg) = probe.requestNext()
              latestValues((probeNr, pid)) shouldEqual (msg - 1)
              latestValues += (probeNr, pid) -> msg
          }
        }
      }
      system.log.info("Received all messages {}", latestValues)
    }

  }
}
