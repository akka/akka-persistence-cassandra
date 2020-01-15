/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.time.{ LocalDate, ZoneOffset }

import akka.cluster.Cluster
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.query.{ EventEnvelope, NoOffset }
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object EventsByTagPubsubSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.actor.serialize-messages = off
    akka.actor.serialize-creators = off
    akka.remote.netty.tcp.port = 0
    akka.remote.artery.canonical.port = 0
    akka.remote.netty.tcp.hostname = "127.0.0.1"
    cassandra-journal {
      pubsub-notification = on
      
      read.refresh-interval = 10s

      events-by-tag {
        flush-interval = 0ms
        eventual-consistency-delay = 0s
      }
    }
    """).withFallback(EventsByTagSpec.config)
}

class EventsByTagPubsubSpec extends CassandraSpec(EventsByTagPubsubSpec.config) {

  val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
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
