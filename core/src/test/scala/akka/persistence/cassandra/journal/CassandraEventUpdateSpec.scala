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
import scala.concurrent.duration._

import scala.concurrent.{ ExecutionContext, Future }

object CassandraEventUpdateSpec {
  val config = ConfigFactory.parseString(
    """
        akka.loglevel = DEBUG
    """
  ).withFallback(CassandraLifecycle.config)
}

class CassandraEventUpdateSpec extends CassandraSpec(CassandraEventUpdateSpec.config) {
  s =>

  private[akka] val log = Logging(system, getClass)
  private val serialization = SerializationExtension(system)

  val shortWait = 10.millis

  val updater = new CassandraEventUpdate {

    override private[akka] val log = s.log
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
      val pid = nextPid
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

    // eventsByTag doesn't include the original tags so if used to transform
    // will wipe out old tags
    "update the event in messages not wiping out existing tags" in {
      val pid = nextPid
      val a = system.actorOf(TestTaggingActor.props(pid, Set("keepme")))
      a ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      expectEventsForTag(tag = "keepme", "e-1")

      val eventsBefore = events(pid)
      eventsBefore.map(_.pr.payload) shouldEqual Seq("e-1")
      val originalEvent = eventsBefore.head
      val modifiedEvent = serialize(
        originalEvent.pr.withPayload("secrets"),
        originalEvent.offset, Set.empty
      )

      updater.updateEvent(modifiedEvent, providingTags = false).futureValue shouldEqual Done

      eventPayloadsWithTags(pid) shouldEqual Seq(("secrets", Set("keepme")))
    }

    "update the event in tag_views" in {
      val pid = nextPid
      val b = system.actorOf(TestTaggingActor.props(pid, Set("red", "blue")))
      b ! "e-1"
      expectMsgType[TestTaggingActor.Ack.type]
      val eventsBefore = events(pid).head
      val modifiedEvent = serialize(
        eventsBefore.pr.withPayload("hidden"), eventsBefore.offset, Set("green", "red", "blue")
      )

      expectEventsForTag(tag = "red", "e-1")
      expectEventsForTag(tag = "blue", "e-1")

      updater.updateEvent(modifiedEvent).futureValue shouldEqual Done

      expectEventsForTag(tag = "red", "hidden")
      expectEventsForTag(tag = "blue", "hidden")
    }

    "deal with tag pid sequence nr not being here" in {
      /*
      If running against a live application then events by persistence id will
      find events that the tag write is buffered. Should deal with that e.g.
      report which ones haven't been updated, offer an API to start from a given
      sequenceNr so it can be run again without duplicating work
       */
      pending
    }

    "add tag to existing event" in {
      // TODO, we need to do this but decide if we want to do it while the app is running
      // will need to be very careful about tag pid sequence nrs
      pending
    }

    def serialize(pr: PersistentRepr, offset: UUID, tags: Set[String]): Serialized = {
      serializeEvent(pr, tags, offset, Hour, serialization, None)
    }
  }
}
