/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import scala.concurrent.Await
import akka.Done
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, TestTaggingActor, _ }
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.actor.ExtendedActorSystem
import akka.stream.alpakka.cassandra.CqlSessionProvider
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

object CassandraEventUpdateSpec {
  val config = ConfigFactory.parseString("""
    """).withFallback(CassandraLifecycle.config)
}

class CassandraEventUpdateSpec extends CassandraSpec(CassandraEventUpdateSpec.config) { s =>

  private[akka] val log = Logging(system, classOf[CassandraEventUpdateSpec])
  private val serialization = SerializationExtension(system)

  val updater = new CassandraEventUpdate {

    override private[akka] val log = s.log
    override private[akka] def settings: PluginSettings =
      PluginSettings(system)
    override private[akka] implicit val ec: ExecutionContext = system.dispatcher
    // use separate session, not shared via CassandraSessionRegistry because init is different
    private val sessionProvider =
      CqlSessionProvider(
        system.asInstanceOf[ExtendedActorSystem],
        system.settings.config.getConfig(PluginSettings.DefaultConfigPath))
    override private[akka] val session: CassandraSession =
      new CassandraSession(
        system,
        sessionProvider,
        ec,
        log,
        systemName,
        init = _ => Future.successful(Done),
        onClose = () => ())
  }

  "CassandraEventUpdate" must {
    "update the event in messages" in {
      val pid = nextPid
      val a = system.actorOf(TestTaggingActor.props(pid))
      a ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      val eventsBefore = events(pid)
      eventsBefore.map(_.pr.payload) shouldEqual Seq("e-1")
      val originalEvent = eventsBefore.head
      val modifiedEvent = serialize(originalEvent.pr.withPayload("secrets"), originalEvent.offset, Set("ignored"))

      updater.updateEvent(modifiedEvent).futureValue shouldEqual Done

      eventPayloadsWithTags(pid) shouldEqual Seq(("secrets", Set()))
    }

    "update the event in tag_views" in {
      val pid = nextPid
      val b = system.actorOf(TestTaggingActor.props(pid, Set("red", "blue")))
      b ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      val eventsBefore = events(pid).head
      val modifiedEvent = serialize(eventsBefore.pr.withPayload("hidden"), eventsBefore.offset, Set("ignored"))

      expectEventsForTag(tag = "red", "e-1")
      expectEventsForTag(tag = "blue", "e-1")

      updater.updateEvent(modifiedEvent).futureValue shouldEqual Done

      expectEventsForTag(tag = "red", "hidden")
      expectEventsForTag(tag = "blue", "hidden")
    }

    def serialize(pr: PersistentRepr, offset: UUID, tags: Set[String]): Serialized = {
      import system.dispatcher
      Await.result(serializeEvent(pr, tags, offset, Hour, serialization, system), remainingOrDefault)
    }
  }
}
