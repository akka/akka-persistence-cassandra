/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.time.LocalDate
import java.time.ZoneOffset

import scala.concurrent.duration._
import scala.util.Try
import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.EventEnvelope
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.cluster.Cluster

import scala.concurrent.Await
import akka.persistence.query.NoOffset

object EventsByTagPubsubSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.actor.serialize-messages = off
    akka.actor.serialize-creators = off
    cassandra-journal {
      pubsub-minimum-interval = 1 millisecond
    }
    cassandra-query-journal {
      refresh-interval = 10s
      eventual-consistency-delay = 0s
    }
    """).withFallback(EventsByTagSpec.config)
}

class EventsByTagPubsubSpec extends TestKit(ActorSystem("EventsByTagPubsubSpec", EventsByTagPubsubSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  override def systemName: String = "EventsByTagPubsubSpec"
  implicit val mat = ActorMaterializer()(system)
  lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
  lazy val session = {
    import system.dispatcher
    Await.result(writePluginConfig.sessionProvider.connect(), 5.seconds)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    Try(session.close())
    Try(session.getCluster.close())
    super.afterAll()
  }

  "Cassandra query getEventsByTag when running clustered with pubsub enabled" must {
    "present new events to an ongoing getEventsByTag stream long before polling would kick in" in {
      val actor = system.actorOf(TestActor.props("EventsByTagPubsubSpec_a"))

      val blackSrc = queries.eventsByTag(tag = "black", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNoMessage(300.millis)

      actor ! "a black car"
      probe.within(5.seconds) { // long before refresh-interval, which is 10s
        probe.expectNextPF { case e @ EventEnvelope(_, _, _, "a black car") => e }
      }
    }
  }
}

