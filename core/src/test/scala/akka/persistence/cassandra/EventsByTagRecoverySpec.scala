/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ ActorSystem, PoisonPill }
import akka.persistence.cassandra.TestTaggingActor.Ack
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

object EventsByTagRecoverySpec {

  val keyspaceName = "EventsByTagRecovery"

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = DEBUG
       |}
       |cassandra-journal {
       |  keyspace = $keyspaceName
       |  log-queries = off
       |  events-by-tag {
       |     max-message-batch-size = 2
       |     bucket-size = "Day"
       |  }
       |}
       |cassandra-snapshot-store.keyspace=CassandraEventsByTagLoadSpecSnapshot
       |
       |cassandra-query-journal = {
       |  first-time-bucket = "20171110"
       |}
       |akka.actor.serialize-messages=off
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)
}

class EventsByTagRecoverySpec extends TestKit(ActorSystem("EventsByTagRecoverySpec", EventsByTagRecoverySpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  import EventsByTagRecoverySpec._

  override protected def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .withClusterName("EventsByTagRecoverySpec")
      .addContactPoint("localhost")
      .withPort(9042)
      .build()
    cluster.connect().execute(s"drop keyspace $keyspaceName")
    cluster.close()
  }

  override def systemName = keyspaceName

  "Events by tag recovery" must {
    "continue tag sequence nrs" in {
      val p1 = system.actorOf(TestTaggingActor.props("p1", Set("blue", "green")))
      (1 to 4) foreach { i =>
        p1 ! s"e-$i"
        expectMsg(Ack)
      }
      p1 ! PoisonPill

      system.terminate().futureValue

      val systemTwo = ActorSystem("s2", EventsByTagRecoverySpec.config)
      implicit val materialiser = ActorMaterializer()(systemTwo)
      try {
        val tProbe = TestProbe()(systemTwo)
        val p1take2 = systemTwo.actorOf(TestTaggingActor.props("p1", Set("blue", "green")))
        (5 to 8) foreach { i =>
          p1take2.tell(s"e-$i", tProbe.ref)
          tProbe.expectMsg(Ack)
        }

        val queryJournal = PersistenceQuery(systemTwo).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
        val greenTags = queryJournal.currentEventsByTag(tag = "green", offset = NoOffset)
        val probe = greenTags.runWith(TestSink.probe[Any](systemTwo))
        probe.request(9)
        (1 to 8) foreach { i =>
          val event = s"e-$i"
          probe.expectNextPF { case EventEnvelope(_, "p1", `i`, `event`) => }
        }
        probe.expectComplete()
      } finally {
        systemTwo.terminate().futureValue
      }
    }
  }
}
