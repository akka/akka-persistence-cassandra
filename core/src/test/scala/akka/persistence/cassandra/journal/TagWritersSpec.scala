/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.nio.ByteBuffer

import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, PoisonPill, Props }
import akka.persistence.cassandra.Hour
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import akka.util.Timeout
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

object TagWritersSpec {
  val config: Config =
    ConfigFactory.parseString("""
      akka {
        use-slf4j = off
        loglevel = INFO
      }
    """)

}

class TagWritersSpec
    extends TestKit(ActorSystem("TagWriterSpec", TagWritersSpec.config))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers {

  val smallWait = 10.milliseconds

  private val defaultSettings = TagWriterSettings(
    maxBatchSize = 10,
    flushInterval = 10.seconds,
    scanningFlushInterval = 20.seconds,
    stopTagWriterWhenIdle = 5.seconds,
    pubsubNotification = Duration.Undefined)

  private def testProps(settings: TagWriterSettings, tagWriterCreator: String => ActorRef): Props =
    Props(new TagWriters(settings, tagWriterSession = null) {
      override def createTagWriter(tag: String): ActorRef = tagWriterCreator(tag)
    })

  val pid = "pid"

  def dummySerialized(tag: String) = {
    val uuid = Uuids.timeBased()
    Serialized(pid, 1L, ByteBuffer.wrap(Array()), Set(tag), "", "", 1, "", None, uuid, TimeBucket(uuid, Hour))
  }

  def initializePid(tagWriters: ActorRef) = {
    tagWriters ! PersistentActorStarting(pid, TestProbe().ref)
    expectMsgType[PersistentActorStartingAck.type]
  }

  "Tag writers" must {

    "reply when all tag writes are complete" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))
      initializePid(tagWriters)

      val blueTagWrite = TagWrite("blue", List(event(pid, 0, "cat", Set("red", "blue"))))
      val redTagWrite = TagWrite("red", List(event(pid, 0, "dog", Set("red", "blue"))))

      tagWriters ! BulkTagWrite(List(blueTagWrite, redTagWrite), Nil)

      expectNoMessage()

      redProbe.expectMsg(redTagWrite)
      blueProbe.expectMsg(blueTagWrite)

      redProbe.reply(Done)
      expectNoMessage()
      blueProbe.reply(Done)
      expectMsg(Done)
    }

    "flush all tag writers" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))
      initializePid(tagWriters)

      // do something to make it create a couple of tag writers
      val blueTagWrite = TagWrite("blue", List(dummySerialized("blue")))
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", List(dummySerialized("red")))
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      tagWriters ! FlushAllTagWriters(Timeout(1.second))
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
      initializePid(tagWriters)

      // do something to make it create a couple
      val blueTagWrite = TagWrite("blue", List(dummySerialized("blue")))
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", List(dummySerialized("ref")))
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      tagWriters ! SetTagProgress("p1", Map("red" -> TagProgress("p1", 2, 1)))
      redProbe.expectMsg(ResetPersistenceId("red", TagProgress("p1", 2, 1)))
      blueProbe.expectMsg(ResetPersistenceId("blue", TagProgress("p1", 0, 0)))
      expectNoMessage(smallWait)
      redProbe.reply(ResetPersistenceIdComplete)
      expectNoMessage(smallWait)
      blueProbe.reply(ResetPersistenceIdComplete)
      expectMsg(TagProcessAck)
    }

    "informs tag writers when persistent actor terminates" in {
      val redProbe = TestProbe()
      val blueProbe = TestProbe()
      val probes = Map("red" -> redProbe, "blue" -> blueProbe)
      val tagWriters = system.actorOf(testProps(defaultSettings, tag => probes(tag).ref))
      initializePid(tagWriters)

      val persistentActor = system.actorOf(Props(new Actor {
        override def receive: Receive = { case _ => }
      }))
      tagWriters ! PersistentActorStarting("pid1", persistentActor)
      expectMsg(PersistentActorStartingAck)

      val blueTagWrite = TagWrite("blue", List(dummySerialized("blue")))
      tagWriters ! blueTagWrite
      blueProbe.expectMsg(blueTagWrite)
      val redTagWrite = TagWrite("red", List(dummySerialized("red")))
      tagWriters ! redTagWrite
      redProbe.expectMsg(redTagWrite)

      persistentActor ! PoisonPill
      blueProbe.expectMsg(DropState("pid1"))
      redProbe.expectMsg(DropState("pid1"))
    }

    "buffer requests when passivating" in {
      val probe = TestProbe()
      val tagWriters = system.actorOf(testProps(defaultSettings, _ => probe.ref))
      initializePid(tagWriters)

      // send a tag write so that it is created
      val preWrite = TagWrite("blue", List(dummySerialized("blue")))
      tagWriters ! preWrite
      probe.expectMsg(preWrite)

      tagWriters.tell(PassivateTagWriter("blue"), probe.ref)
      probe.expectMsg(StopTagWriter)

      tagWriters ! FlushAllTagWriters(Timeout(remainingOrDefault))
      // passivate in progress, so Flush is buffered
      probe.expectNoMessage(100.millis)

      val blueTagWrite = TagWrite("blue", List(dummySerialized("blue")))
      tagWriters ! blueTagWrite
      probe.expectNoMessage(100.millis)

      tagWriters.tell(CancelPassivateTagWriter("blue"), probe.ref)
      probe.expectMsg(Flush)
      probe.reply(FlushComplete)
      expectMsg(AllFlushed)

      probe.expectMsg(blueTagWrite)
    }
  }

  private def event(pid: String, seqNr: Long, payload: String, tags: Set[String]): Serialized = {
    val uuid = Uuids.timeBased()
    Serialized(pid, seqNr, ByteBuffer.wrap(payload.getBytes()), tags, "", "", 1, "", None, uuid, TimeBucket(uuid, Hour))
  }

  override protected def afterAll(): Unit =
    shutdown()
}
