/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.Done
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, TestTaggingActor, _ }
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ ExecutionContext, Future }

object CassandraEventUpdateSpec {
  val config = ConfigFactory.parseString(
    """
    """
  ).withFallback(CassandraLifecycle.config)
}

class CassandraEventUpdateSpec extends CassandraSpec(CassandraEventUpdateSpec.config) {

  private val log = Logging(system, getClass)
  private val serialization = SerializationExtension(system)

  val updater = new CassandraEventUpdate {
    override private[akka] def config: CassandraJournalConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
    override private[akka] implicit val ec: ExecutionContext = system.dispatcher
    override private[akka] val session: CassandraSession = new CassandraSession(
      system,
      config.sessionProvider,
      config.sessionSettings,
      ec,
      log,
      systemName,
      init = _ => Future.successful(Done)
    )
  }

  "CassandraEventUpdate" must {
    "update the event in messages" in {
      val pid = "a"
      val a = system.actorOf(TestTaggingActor.props(pid))
      a ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      val eventsBefore = events(pid)
      eventsBefore.map(_.pr.payload) shouldEqual Seq("e-1")
      val originalEvent = eventsBefore.head
      val modifiedEvent = serialize(
        originalEvent.pr.withPayload("secrets"),
        originalEvent.offset, Set("captain america")
      )

      updater.updateEvent(modifiedEvent).futureValue shouldEqual Done

      eventPayloadsWithTags(pid) shouldEqual Seq(("secrets", Set("captain america")))
    }

    "update the event in tag_views" in {
      val pid = "b"
      val b = system.actorOf(TestTaggingActor.props(pid, Set("red", "blue")))
      b ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      val eventsBefore = events(pid).head
      val modifiedEvnet = serialize(
        eventsBefore.pr.withPayload("hidden"), eventsBefore.offset, Set("red", "blue")
      )

      // update the event
    }

    "remove tags" in {
      // FIXME, decide if want to do this
      pending
    }

    "add tag" in {
      // FIXME, we need to do this but decide if we want to do it while the app is running
      // will need to be very careful about tag pid sequence nrs
      pending
    }

    def serialize(pr: PersistentRepr, offset: UUID, tags: Set[String]): Serialized = {
      serializeEvent(pr, tags, offset, Hour, serialization, None)
    }
  }
}
