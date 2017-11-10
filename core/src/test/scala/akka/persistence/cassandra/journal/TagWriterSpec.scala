/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.nio.ByteBuffer
import java.util.UUID

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.journal.TagWriter.{ TagProgress, TagWriterSession, TagWriterSettings, TagWrites }
import akka.persistence.cassandra.journal.TagWriterSpec.{ FirstTagWrite, ProgressWrite }
import akka.testkit.{ TestKit, TestProbe }
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ PreparedStatement, Statement }
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }

object TagWriterSpec {
  val config = ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = DEBUG
      |  actor {
      |    debug {
      |      # enable function of LoggingReceive, which is to log any received message at
      |      # DEBUG level
      |      receive = on
      |      unhandled = on
      |    }
      |  }
      |}
    """.stripMargin
  )

  case class ProgressWrite(persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)
  case class FirstTagWrite(persistenceId: String, bucket: TimeBucket)
}

/**
 * We have a lot of integration tests around eventsByTag queries so
 * writing this against a fake CassandraSession to test the batching
 */
class TagWriterSpec extends TestKit(ActorSystem("TagWriterSpec", TagWriterSpec.config))
  with WordSpecLike {

  val fakePreparedStatement: Future[PreparedStatement] = Future.successful(null)
  val successfulWrite: Statement => Future[Done] = _ => Future.successful(Done)
  val defaultSettings = TagWriterSettings(
    maxBatchSize = 10,
    10.seconds,
    pubsubNotification = false
  )
  val waitDuration = 100.millis
  val shortDuration = 10.millis

  "Tag writer batching" must {
    "not write until batch has reached capacity" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 2))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrites(Vector(e1))
      probe.expectNoMessage(waitDuration)

      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrites(Vector(e2))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1), (e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))

      val e3 = event("p1", 3L, "e-2", bucket)
      ref ! TagWrites(Vector(e3))
      probe.expectNoMessage(waitDuration)
    }

    "write multiple persistenceIds in the same batch" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val p1e1 = event("p1", 1L, "e-1", bucket)
      val p1e2 = event("p1", 2L, "e-2", bucket)
      val p2e1 = event("p2", 1L, "e-1", bucket)
      ref ! TagWrites(Vector(p1e1, p1e2, p2e1))
      // for tag_views table they are the same c* partition
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(FirstTagWrite("p2", bucket))
      probe.expectMsg(Vector((p1e1, 1), (p1e2, 2), (p2e1, 1)))
      // for the progress table they different c* partitions so separate writes
      probe.expectMsg(ProgressWrite("p1", 2, 2, p1e2.timeUuid))
      probe.expectMsg(ProgressWrite("p2", 1, 1, p2e1.timeUuid))
    }

    "flush after interval" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 2, flushInterval = 500.millis))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrites(Vector(e1))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrites(Vector(e2))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(Vector((e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
    }

    "flush when time bucket changes" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)
      val nextBucket = bucket.next()

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrites(Vector(e1))
      probe.expectNoMessage(100.millis)

      val e2 = event("p1", 2L, "e-2", nextBucket)
      ref ! TagWrites(Vector(e2))
      // Buckets are separate partitions, so only send the first
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      val e3 = event("p1", 3L, "e-3", nextBucket)
      val e4 = event("p1", 4L, "e-4", nextBucket)
      ref ! TagWrites(Vector(e3, e4))
      // batch size has been hit now
      probe.expectMsg(Vector((e2, 2), (e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "flush if time bucket changes within a single msg" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)
      val nextBucket = bucket.next()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", nextBucket)
      ref ! TagWrites(Vector(e1, e2))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      probe.expectNoMessage(waitDuration)

      val e3 = event("p1", 3L, "e-3", nextBucket)
      val e4 = event("p1", 4L, "e-4", nextBucket)
      ref ! TagWrites(Vector(e3, e4))
      // batch size has now been hit
      probe.expectMsg(Vector((e2, 2), (e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "not execute query N+1 while query N is outstanding" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = promiseForWrite.future,
        settings = defaultSettings.copy(maxBatchSize = 2)
      )
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrites(Vector(e1, e2, e3, e4))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1), (e2, 2)))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector((e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "flush time buckets one by one if arrive in same msg" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = promiseForWrite.future,
        settings = defaultSettings.copy(maxBatchSize = 2)
      )
      val now = UUIDs.timeBased()
      val bucketOne = TimeBucket(now)
      val bucketTwo = bucketOne.next()
      val bucketThree = bucketTwo.next()

      val e1 = event("p1", 1L, "e-1", bucketOne)
      val e2 = event("p1", 2L, "e-2", bucketTwo)
      val e3 = event("p1", 3L, "e-3", bucketThree)
      val e4 = event("p1", 4L, "e-4", bucketThree)

      ref ! TagWrites(Vector(e1, e2, e3))
      probe.expectMsg(FirstTagWrite("p1", bucketOne))
      probe.expectMsg(Vector((e1, 1)))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      probe.expectMsg(Vector((e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectNoMessage(waitDuration)
      // fill up batch to flush out the last one
      ref ! TagWrites(Vector(e4))
      probe.expectMsg(Vector((e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "flush immediately if interval set to 0" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(flushInterval = Duration.Zero))
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrites(Vector(e1))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(shortDuration, Vector((e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      ref ! TagWrites(Vector(e2))
      probe.expectMsg(shortDuration, Vector((e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
    }

    "not flush if write in progress" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = promiseForWrite.future,
        settings = defaultSettings.copy(maxBatchSize = 2)
      )
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrites(Vector(e1, e2))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1), (e2, 2)))
      ref ! TagWrites(Vector(e3, e4))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector((e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "not flush if write in progress with no interval" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = promiseForWrite.future,
        settings = defaultSettings.copy(maxBatchSize = 3, flushInterval = 0.millis)
      )
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrites(Vector(e1, e2))
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1), (e2, 2)))
      ref ! TagWrites(Vector(e3, e4))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector((e3, 3), (e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "not flush if internal flush is in progress" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = promiseForWrite.future,
        settings = defaultSettings.copy(maxBatchSize = 2, flushInterval = 500.millis)
      )
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrites(Vector(e1))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(FirstTagWrite("p1", bucket))
      probe.expectMsg(Vector((e1, 1)))

      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      ref ! TagWrites(Vector(e2, e3))
      // should not be written right away and there should not be another flush
      // scheduled
      probe.expectNoMessage(600.millis)

      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid)) // from previous write now we've completed the promise
      probe.expectMsg(Vector((e2, 2), (e3, 3)))
      probe.expectMsg(ProgressWrite("p1", 3, 3, e3.timeUuid))
    }

    "resume from existing sequence nr" in {
      val progress = Future.successful(Some(TagProgress(
        "p1", 100, 10
      )))
      val (probe, ref) = setup(
        settings = defaultSettings.copy(maxBatchSize = 1),
        sequenceNrLookup = progress
      )
      val now = UUIDs.timeBased()
      val bucket = TimeBucket(now)

      val e1 = event("p1", 101L, "e-1", bucket)
      ref ! TagWrites(Vector(e1))
      // no first write msg
      probe.expectMsg(Vector((e1, 11)))
      probe.expectMsg(ProgressWrite("p1", 101, 11, e1.timeUuid))

      val e2 = event("p1", 102L, "e-2", bucket)
      ref ! TagWrites(Vector(e2))
      probe.expectMsg(Vector((e2, 12)))
      probe.expectMsg(ProgressWrite("p1", 102, 12, e2.timeUuid))
    }
  }

  "Tag writer error scenarios" must {
    "Receive a older bucket for the same persistenceId" in {
      pending
    }
    "Cassandra write fails" in {
      pending
    }
  }

  private def setup(
    tag:                   String                      = "tag-1",
    settings:              TagWriterSettings           = defaultSettings,
    writeResponse:         Future[Done]                = Future.successful(Done),
    progressWriteResponse: Future[Done]                = Future.successful(Done),
    sequenceNrLookup:      Future[Option[TagProgress]] = Future.successful(None)
  ): (TestProbe, ActorRef) = {
    val probe = TestProbe()
    val session = new TagWriterSession(tag, fakePreparedStatement, successfulWrite, null, fakePreparedStatement,
      fakePreparedStatement, fakePreparedStatement) {
      override def writeBatch(events: Seq[(Serialized, Long)])(implicit ec: ExecutionContext) = {
        probe.ref ! events
        writeResponse
      }

      override def writeProgress(pid: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(implicit ec: ExecutionContext): Future[Done] = {
        probe.ref ! ProgressWrite(pid, seqNr, tagPidSequenceNr, offset)
        progressWriteResponse
      }

      override def writeTagFirstBucket(pid: String, bucket: TimeBucket)(implicit ec: ExecutionContext): Future[Done] = {
        probe.ref ! FirstTagWrite(pid, bucket)
        progressWriteResponse
      }

      override def selectTagProgress(pid: String)(implicit ec: ExecutionContext): Future[Option[TagProgress]] = {
        sequenceNrLookup
      }
    }

    val ref = system.actorOf(TagWriter.props(session, tag, settings))
    (probe, ref)
  }

  private def event(pId: String, seqNr: Long, payload: String, bucket: TimeBucket, tags: Set[String] = Set()): Serialized = {
    val s = Serialized(pId, seqNr, ByteBuffer.wrap(payload.getBytes()), tags, "", "", 1, "", None, UUIDs.timeBased(), bucket)
    s
  }
}
