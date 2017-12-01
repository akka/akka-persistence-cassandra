/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.ActorSystem
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters.{ AllFlushed, FlushAllTagWriters, TagFlush }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.duration._

class TagWritersSpec extends TestKit(ActorSystem("TagWriterSpec"))
  with WordSpecLike
  with BeforeAndAfterAll
  with ImplicitSender
  with Matchers {

  "Tag writers" must {
    "forward flush requests" in {
      val probe = TestProbe()
      val tagWriters = system.actorOf(TagWriters.props((_, tag) => {
        tag shouldEqual "blue"
        probe.ref
      }))

      tagWriters ! TagFlush("blue")
      probe.expectMsg(Flush)
      probe.reply(Flushed)
      expectMsg(Flushed) // should be forwarded
    }

    "flush all tag writers" in {
      val probe = TestProbe()
      val tagWriters = system.actorOf(TagWriters.props((_, tag) => probe.ref))

      // do something to make it create a couple
      val tp = TagProgress("p", 1, 1)
      tagWriters ! SetTagProgress("blue", tp)
      probe.expectMsg(SetTagProgress("blue", tp))
      tagWriters ! SetTagProgress("red", tp)
      probe.expectMsg(SetTagProgress("red", tp))

      tagWriters ! FlushAllTagWriters
      probe.expectMsg(Flush)
      probe.reply(Flushed)
      expectNoMessage(100.millis)
      probe.expectMsg(Flush)
      probe.reply(Flushed)
      expectMsg(AllFlushed)
    }
  }

  override protected def afterAll(): Unit = {
    shutdown()
  }
}
