/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.nio.ByteBuffer
import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.cassandra.Day
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, TagPidSequenceNr }
import akka.persistence.cassandra.journal.TagWriter.AwaitingWrite
import akka.testkit.{ TestKit, TestProbe }
import akka.util.OptionVal
import com.datastax.oss.driver.api.core.uuid.Uuids
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BufferSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  implicit val system = ActorSystem()

  "Buffer" should {
    "not write when empty" in {
      Buffer.empty(2).shouldWrite() shouldEqual false
    }
    "write when batch size met (single AwaitingWrite)" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)

      Buffer.empty(2).add(AwaitingWrite(List((e1, 1), (e2, 2)), OptionVal.None)).shouldWrite() shouldEqual true
    }
    "write when batch size met (multiple AwaitingWrites)" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)

      val buffer = Buffer
        .empty(2)
        .add(AwaitingWrite(List((e1, 1)), OptionVal.None))
        .add(AwaitingWrite(List((e2, 12)), OptionVal.None))

      buffer.shouldWrite() shouldEqual true
      buffer.nextBatch shouldEqual (List(
        AwaitingWrite(List((e1, 1)), OptionVal.None),
        AwaitingWrite(List((e2, 12)), OptionVal.None)))
    }
    "write when time bucket changes" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket.next())

      Buffer
        .empty(10)
        .add(AwaitingWrite(List((e1, 1)), OptionVal.None))
        .add(AwaitingWrite(List((e2, 12)), OptionVal.None))
        .shouldWrite() shouldEqual true
    }

    "write when time bucket changes in the same add" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket.next())
      val sender = TestProbe().ref

      val buffer = Buffer.empty(3).add(aw(sender, (e1, 1), (e2, 2))) // same aw, spanning time buckets

      buffer.writeRequired shouldEqual true
      buffer.nextBatch shouldEqual Vector(awNoSender((e1, 1)))

      val nextBuffer = buffer.writeComplete()
      nextBuffer.writeRequired shouldEqual false
      nextBuffer.nextBatch shouldEqual Vector(aw(sender, (e2, 2)))

    }
    "write when multiple time buckets" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket.next())
      val e3 = event("p1", 3L, "e-2", bucket.next().next())
      val sender = TestProbe().ref

      val buffer = Buffer.empty(3).add(aw(sender, (e1, 1), (e2, 2), (e3, 3))) // same aw, spanning time buckets

      buffer.writeRequired shouldEqual true
      buffer.nextBatch shouldEqual Vector(awNoSender((e1, 1)))
      buffer.pending shouldEqual Vector(awNoSender((e2, 2)), aw(sender, (e3, 3)))

      val nextBuffer = buffer.writeComplete()
      nextBuffer.writeRequired shouldEqual true
      nextBuffer.nextBatch shouldEqual Vector(awNoSender((e2, 2)))
      nextBuffer.pending shouldEqual Vector(aw(sender, (e3, 3)))

      val nextNextBuffer = nextBuffer.writeComplete()
      nextNextBuffer.writeRequired shouldEqual false
      nextNextBuffer.nextBatch shouldEqual Vector(aw(sender, (e3, 3)))
      nextNextBuffer.pending shouldEqual Vector()
    }

    "break up writes based on batch" in {
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-2", bucket)
      val sender = TestProbe().ref

      val buffer = Buffer.empty(2).add(aw(sender, (e1, 1), (e2, 2), (e3, 3), (e4, 4))) // same aw, greater than batch size

      buffer.writeRequired shouldEqual true
      buffer.nextBatch shouldEqual Vector(awNoSender((e1, 1), (e2, 2)))

      val nextBuffer = buffer.writeComplete()
      nextBuffer.nextBatch shouldEqual Vector(aw(sender, (e3, 3), (e4, 4)))
      nextBuffer.writeRequired shouldEqual true

      val nextNextBuffer = nextBuffer.writeComplete()
      nextNextBuffer.writeRequired shouldEqual false
    }

    // there will never be events in the same add that are out of order  as they are from the same pid
    // but there can be from different adds
    "handle out of order writes" in {
      val currentBucket = (0 to 2).map { _ =>
        val uuid = Uuids.timeBased()
        (uuid, TimeBucket(uuid, Day))
      }
      val sender1 = TestProbe().ref
      val sender2 = TestProbe().ref
      val sender3 = TestProbe().ref

      val futureBucketMillis = Uuids.unixTimestamp(currentBucket(0)._1) + Day.durationMillis
      val futureBucket = TimeBucket(futureBucketMillis, Day)

      val p1e1 = event("p1", 1, "p1-e1", currentBucket(0)._2, uuid = currentBucket(0)._1)
      val p2e1 = event("p2", 1, "p2-e1", futureBucket, uuid = Uuids.startOf(futureBucketMillis))
      val p1e2 = event("p1", 2, "p1-e2", currentBucket(1)._2, uuid = currentBucket(1)._1)

      val buffer = Buffer.empty(2).add(aw(sender1, (p1e1, 1))).add(aw(sender2, (p2e1, 1)))

      buffer.shouldWrite() shouldEqual true
      buffer.nextBatch shouldEqual Vector(aw(sender1, (p1e1, 1)))

      val nextBuffer = buffer.writeComplete()
      nextBuffer.shouldWrite() shouldEqual false
      nextBuffer.nextBatch shouldEqual Vector(aw(sender2, (p2e1, 1)))

      // this is from before what is currently in nextWrite, they should be switched around
      val nextNextBuffer = nextBuffer.add(aw(sender3, (p1e2, 2)))
      nextNextBuffer.shouldWrite() shouldEqual true
      nextNextBuffer.nextBatch shouldEqual Vector(aw(sender3, (p1e2, 2)))

      val finalBuffer = nextNextBuffer.writeComplete()
      finalBuffer.shouldWrite() shouldEqual false
      nextBuffer.nextBatch shouldEqual Vector(aw(sender2, (p2e1, 1)))

    }

    "handle being overloaded" in {
      val bucket = nowBucket()
      var buffer = Buffer.empty(2)
      val totalWrites = 1000000
      for (i <- 1 to totalWrites) {
        buffer = buffer.add(awNoSender((event("p1", seqNr = i, payload = "cats", bucket), i)))
      }
      var writes = 0
      while (buffer.shouldWrite()) {
        writes += 1
        buffer = buffer.writeComplete()
      }
      writes shouldEqual totalWrites / 2
    }
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def awNoSender(events: (Serialized, TagPidSequenceNr)*): AwaitingWrite = {
    AwaitingWrite(events.toList, OptionVal.None)
  }

  private def aw(sender: ActorRef, events: (Serialized, TagPidSequenceNr)*): AwaitingWrite = {
    AwaitingWrite(events.toList, OptionVal(sender))
  }

  private def event(
      pId: String,
      seqNr: Long,
      payload: String,
      bucket: TimeBucket,
      tags: Set[String] = Set(),
      uuid: UUID = Uuids.timeBased()): Serialized =
    Serialized(pId, seqNr, ByteBuffer.wrap(payload.getBytes()), tags, "", "", 1, "", None, uuid, bucket)

  private def nowBucket(): TimeBucket = {
    val now = Uuids.timeBased()
    TimeBucket(now, Day)
  }

}
