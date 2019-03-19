/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ Actor, ActorRef, ActorSystem, PoisonPill, Props }
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.duration._

class TagWritersSpec
    extends TestKit(ActorSystem("TagWriterSpec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers {

  val smallWait = 10.milliseconds

  private val defaultSettings = TagWriterSettings(
    maxBatchSize = 10,
    flushInterval = 10.seconds,
    scanningFlushInterval = 20.seconds,
    pubsubNotification = false)

  private def testProps(settings: TagWriterSettings, tagWriterCreator: String => ActorRef): Props =
    Props(new TagWriters(settings, tagWriterSession = null) {
      override def createTagWriter(tag: String): ActorRef = tagWriterCreator(tag)
    })

  "Tag writers" must {
    "forward flush requests" in {
      val probe = TestProbe()
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => {
        tag shouldEqual "blue"
        probe.ref
      }))

      tagWriters ! TagFlush("blue")
      probe.expectMsg(Flush)
      probe.reply(FlushComplete)
      expectMsg(FlushComplete) // should be forwarded
    }

    "flush all tag writers" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))

      // do something to make it create a couple
      val blueTagWrite = TagWrite("blue", Vector.empty)
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", Vector.empty)
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      tagWriters ! FlushAllTagWriters
      blueProbe.expectMsg(Flush)
      blueProbe.reply(FlushComplete)
      redProbe.expectMsg(Flush)
      // All flushed comes after all have flushes
      expectNoMessage(100.millis)
      redProbe.reply(FlushComplete)
      expectMsg(AllFlushed)
    }

    "sends blank progress to tag writers not in recovery" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))

      // do something to make it create a couple
      val blueTagWrite = TagWrite("blue", Vector.empty)
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", Vector.empty)
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      tagWriters ! PersistentActorStarting("p1", Map("red" -> TagProgress("p1", 2, 1)), system.deadLetters)
      redProbe.expectMsg(ResetPersistenceId("red", TagProgress("p1", 2, 1)))
      blueProbe.expectMsg(ResetPersistenceId("blue", TagProgress("p1", 0, 0)))
      expectNoMessage(smallWait)
      redProbe.reply(ResetPersistenceIdComplete)
      expectNoMessage(smallWait)
      blueProbe.reply(ResetPersistenceIdComplete)
      expectMsg(PersistentActorStartingAck)
    }

    "informs tag writers when persistent actor terminates" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))

      val persistentActor = system.actorOf(Props(new Actor {
        override def receive: Receive = { case _ => }
      }))
      tagWriters ! PersistentActorStarting("pid1", Map.empty, persistentActor)
      expectMsg(PersistentActorStartingAck)

      val blueTagWrite = TagWrite("blue", Vector.empty)
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", Vector.empty)
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      persistentActor ! PoisonPill
      blueProbe.expectMsg(DropState("pid1"))
      redProbe.expectMsg(DropState("pid1"))
    }
  }

  override protected def afterAll(): Unit =
    shutdown()
}
