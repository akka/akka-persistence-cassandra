/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration._
import scala.util.Try
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal.Tagged
import akka.persistence.journal.WriteEventAdapter
import akka.persistence.query.EventEnvelope
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.cluster.Cluster

object EventsByTagPubsubSpec {
  val today = LocalDate.now(ZoneOffset.UTC)

  val config = ConfigFactory.parseString(s"""
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
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
  import EventsByTagSpec._

  override def systemName: String = "EventsByTagPubsubSpec"
  implicit val mat = ActorMaterializer()(system)
  lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  val writePluginConfig = new CassandraJournalConfig(system.settings.config.getConfig("cassandra-journal"))
  lazy val session = {
    val cluster = writePluginConfig.clusterBuilder.build
    cluster.connect()
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

      val blackSrc = queries.eventsByTag(tag = "black", offset = 0L)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNoMsg(300.millis)

      actor ! "a black car"
      probe.within(5.seconds) { // long before refresh-interval, which is 10s
        probe.expectNextPF { case e @ EventEnvelope(_, _, _, "a black car") => e }
      }
    }
  }
}

