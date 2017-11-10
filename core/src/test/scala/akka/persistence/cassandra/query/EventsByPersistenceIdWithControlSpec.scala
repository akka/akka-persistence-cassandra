/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time._
import org.scalatest.{Matchers, WordSpecLike}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._

object EventsByPersistenceIdWithControlSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    cassandra-journal.keyspace=EventsByPersistenceIdWithControlSpec
    cassandra-query-journal.refresh-interval = 120s # effectively disabled
    cassandra-query-journal.max-result-size-query = 20
    cassandra-journal.target-partition-size = 15
    akka.stream.materializer.max-input-buffer-size = 4 # there is an async boundary
    """).withFallback(CassandraLifecycle.config)
}

class EventsByPersistenceIdWithControlSpec
  extends TestKit(ActorSystem("EventsByPersistenceIdWithControlSpec", EventsByPersistenceIdWithControlSpec.config))
  with ScalaFutures
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  override def systemName: String = "EventsByPersistenceIdWithControlSpec"

  implicit val mat = ActorMaterializer()(system)

  implicit val patience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Milliseconds))

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val noMsgTimeout = 100.millis

  def setup(persistenceId: String, n: Int): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for (i <- 1 to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  @tailrec private def fish(probe: Probe[Any], expect: String, found: Set[Any] = Set.empty): Set[Any] = {
    found.size should be < 100
    val next = probe.expectNext()
    if (next == expect)
      found + expect
    else
      fish(probe, expect, found + next)
  }

  "Cassandra query EventsByPersistenceIdWithControl" must {
    "find new events" in {
      val ref = setup("a", 3)

      val src: Source[EventEnvelope, Future[EventsByPersistenceIdStage.Control]] =
        queries.eventsByPersistenceIdWithControl("a", 0L, Long.MaxValue)

      val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
      val control = futureControl.futureValue
      control.poll(3)

      probe
        .request(5)
        .expectNext("a-1", "a-2", "a-3")

      ref ! "a-4"
      expectMsg("a-4-done")
      control.poll(4)
      probe.expectNext("a-4")

      probe.cancel()
    }

    "find new events when ResultSet not exhausted" in {
      // to test this scenario the async buffer should be filled, so that ResultSet is not exhausted
      val ref = setup("b", 8)

      val src = queries.eventsByPersistenceIdWithControl("b", 0L, Long.MaxValue)
      val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
      val control = futureControl.futureValue
      control.poll(8)

      probe
        .request(2)
        .expectNext("b-1", "b-2")
        .expectNoMessage(noMsgTimeout)

      ref ! "b-9"
      expectMsg("b-9-done")
      control.poll(9)
      probe.expectNoMessage(noMsgTimeout)
      probe.request(10)
      probe.expectNext("b-3", "b-4", "b-5", "b-6", "b-7", "b-8")
      probe.expectNext("b-9")

      probe.cancel()
    }

    "be able to fast forward" in {
      val ref = setup("c", 2)

      val src = queries.eventsByPersistenceIdWithControl("c", 0L, Long.MaxValue)
      val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
      val control = futureControl.futureValue
      control.poll(2)

      probe
        .request(10)
        .expectNext("c-1", "c-2")
        .expectNoMessage(noMsgTimeout)

      control.fastForward(5)
      probe.expectNoMessage(noMsgTimeout)
      ref ! "c-3"
      expectMsg("c-3-done")
      ref ! "c-4"
      expectMsg("c-4-done")
      ref ! "c-5"
      expectMsg("c-5-done")
      ref ! "c-6"
      expectMsg("c-6-done")
      probe.expectNoMessage(noMsgTimeout)
      control.poll(6)
      probe.expectNext("c-5", "c-6")

      probe.cancel()
    }

    "be able to fast forward when ResultSet not exhausted" in {
      // to test this scenario the async buffer should be filled, so that ResultSet is not exhausted
      val ref = setup("d", 12)

      val src = queries.eventsByPersistenceIdWithControl("d", 0L, Long.MaxValue)
      val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
      val control = futureControl.futureValue
      control.poll(12)

      probe
        .request(2)
        .expectNext("d-1", "d-2")
        .expectNoMessage(noMsgTimeout)

      ref ! "d-13"
      expectMsg("d-13-done")
      control.fastForward(12)
      probe.expectNoMessage(noMsgTimeout)
      control.poll(13)
      probe.request(10)

      // due to the async boundary there might be some in flight already before 12
      fish(probe, "d-12") should not contain "d-11"

      probe.expectNext("d-13")
      probe.cancel()
    }

    "be able to fast forward over partition boundaries" in {
      // partition size 15
      setup("e", 35)

      val src = queries.eventsByPersistenceIdWithControl("e", 0L, Long.MaxValue)
      val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
      val control = futureControl.futureValue
      control.poll(35)

      probe.request(10)
      probe.expectNextN((1 to 10).map(i => s"e-$i"))

      control.fastForward(20)
      control.poll(35)
      probe.request(100)
      fish(probe, "e-20") should not contain "e-19"
      probe.expectNextN((21 to 35).map(i => s"e-$i"))

      probe.cancel()
    }
  }
}
