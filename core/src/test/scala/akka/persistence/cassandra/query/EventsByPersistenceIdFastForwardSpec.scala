/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.{ Matchers, WordSpecLike }

object EventsByPersistenceIdFastForwardSpec {

  // separate from EventsByPersistenceIdWithControlSpec since it needs the refreshing enabled
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    cassandra-journal.keyspace=EventsByPersistenceIdFastForwardSpec
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15
    """).withFallback(CassandraLifecycle.config)
}

class EventsByPersistenceIdFastForwardSpec
  extends TestKit(ActorSystem("EventsByPersistenceIdSpec", EventsByPersistenceIdFastForwardSpec.config))
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers
  with ScalaFutures
  with DirectWriting {

  override def systemName: String = "EventsByPersistenceIdFastForwardSpec"

  implicit val mat = ActorMaterializer()(system)

  implicit val patience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Milliseconds))

  lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  "be able to fast forward when currently looking for missing sequence number" in {
    val w1 = UUID.randomUUID().toString
    val evt1 = PersistentRepr("e-1", 1L, "f", "", writerUuid = w1)
    writeTestEvent(evt1)

    val src = queries.eventsByPersistenceIdWithControl("f", 0L, Long.MaxValue)
    val (futureControl, probe) = src.map(_.event).toMat(TestSink.probe[Any])(Keep.both).run()
    val control = futureControl.futureValue
    probe.request(5)

    val evt3 = PersistentRepr("e-3", 3L, "f", "", writerUuid = w1)
    writeTestEvent(evt3)

    probe.expectNext("e-1")

    system.log.debug("Sleeping for query to go into look-for-missing-seqnr-mode")
    Thread.sleep(2000)

    // then we fast forward past the gap
    control.fastForward(3L)
    probe.expectNext("e-3")

    val evt2 = PersistentRepr("e-2", 2L, "f", "", writerUuid = w1)
    val evt4 = PersistentRepr("e-4", 4L, "f", "", writerUuid = w1)
    writeTestEvent(evt2)
    writeTestEvent(evt4)
    probe.expectNext("e-4")

    probe.cancel()
  }
}

