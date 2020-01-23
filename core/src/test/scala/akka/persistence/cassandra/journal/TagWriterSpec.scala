/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.nio.ByteBuffer
import java.util.UUID

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging.Warning
import akka.persistence.cassandra.Day
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters.TagWrite
import akka.persistence.cassandra.journal.TagWriterSpec.{ EventWrite, ProgressWrite, TestEx }
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal.TagWriters.TagWritersSession
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Statement }
import com.datastax.oss.driver.api.core.uuid.Uuids
import com.github.ghik.silencer.silent
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, WordSpecLike }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NoStackTrace

object TagWriterSpec {
  val config =
    ConfigFactory.parseString("""
      |akka {
      |  actor {
      |    debug {
      |      # enable function of LoggingReceive, which is to log any received message at
      |      # DEBUG level
      |      receive = on
      |      unhandled = on
      |    }
      |  }
      |}
    """.stripMargin)

  case class ProgressWrite(persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)
  case class EventWrite(persistenceId: String, seqNr: Long, tagPidSequenceNr: Long)

  case class TestEx(msg: String) extends RuntimeException(msg) with NoStackTrace
}

/**
 * We have a lot of integration tests around eventsByTag queries so
 * writing this against a fake CassandraSession to test the batching
 */
@silent // re-write to use lazy list
class TagWriterSpec
    extends TestKit(ActorSystem("TagWriterSpec", TagWriterSpec.config))
    with WordSpecLike
    with BeforeAndAfterEach
    with ImplicitSender
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit =
    shutdown()

  val fakePs: Future[PreparedStatement] = Future.successful(null)
  val successfulWrite: Statement[_] => Future[Done] = _ => Future.successful(Done)
  val defaultSettings = TagWriterSettings(
    maxBatchSize = 10,
    flushInterval = 10.seconds,
    scanningFlushInterval = 20.seconds,
    stopTagWriterWhenIdle = 5.seconds,
    pubsubNotification = Duration.Undefined)
  val waitDuration = 100.millis
  val shortDuration = 50.millis
  val tagName = "tag-1"
  val bucketSize = Day

  val logProbe = TestProbe()
  system.eventStream.subscribe(logProbe.ref, classOf[Warning])
  system.eventStream.subscribe(logProbe.ref, classOf[akka.event.Logging.Error])

  override protected def afterEach(): Unit = {
    // check for the buffer exceeded log (and other issues)
    logProbe.expectNoMessage(100.millis)
    super.afterEach()
  }

  "Tag writer batching" must {

    "external flush when idle" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 100, flushInterval = 1.hour))
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(waitDuration)
      ref ! Flush
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      expectMsg(FlushComplete)
    }

    "external flush when write in progress and messages in buffer" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2))
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectNoMessage(waitDuration)

      val flushSender = TestProbe("flushSender")
      ref ! TagWrite(tagName, Vector(e3))
      ref.tell(Flush, flushSender.ref)
      ref ! TagWrite(tagName, Vector(e4)) // check adding to the buffer while in progress doesn't lose the flush
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      // only happened due to the flush
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
      flushSender.expectMsg(FlushComplete)
    }

    "external flush when write in progress and receiving a ResetPersistenceId" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 1))
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)

      ref ! TagWrite(tagName, Vector(e1))
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectNoMessage(waitDuration)

      val flushSender = TestProbe("flushSender")
      ref ! TagWrite(tagName, Vector(e2))
      ref.tell(Flush, flushSender.ref)
      ref ! ResetPersistenceId("tag-1", TagProgress("some-other-pid", 2, 4)) // check this preserves flush request
      expectMsg(ResetPersistenceIdComplete)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      probe.expectMsg(Vector(toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      flushSender.expectMsg(FlushComplete)
    }

    "flush on demand when query in progress and no messages in buffer" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2))
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectNoMessage(waitDuration)

      val flushSender = TestProbe("flushSender")
      ref.tell(Flush, flushSender.ref)
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      flushSender.expectMsg(FlushComplete)
    }

    "not write until batch has reached capacity" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 2))
      val bucket = nowBucket()
      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(waitDuration)

      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrite(tagName, Vector(e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))

      val e3 = event("p1", 3L, "e-2", bucket)
      ref ! TagWrite(tagName, Vector(e3))
      probe.expectNoMessage(waitDuration)
    }

    "write multiple persistenceIds in the same batch" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val bucket = nowBucket()
      val p1e1 = event("p1", 1L, "e-1", bucket)
      val p1e2 = event("p1", 2L, "e-2", bucket)
      val p2e1 = event("p2", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(p1e1, p1e2, p2e1))
      // for tag_views table they are the same c* partition
      probe.expectMsg(Vector(toEw(p1e1, 1), toEw(p1e2, 2), toEw(p2e1, 1)))
      // for the progress table they different c* partitions so separate writes
      probe.expectMsg(ProgressWrite("p1", 2, 2, p1e2.timeUuid))
      probe.expectMsg(ProgressWrite("p2", 1, 1, p2e1.timeUuid))
    }

    "flush after interval" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 2, flushInterval = 500.millis))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrite(tagName, Vector(e2))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(Vector(toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
    }

    "flush after interval when new events are written" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 100, flushInterval = 500.millis))
      val bucket = nowBucket()

      val allEvents =
        (1 to 6).foldLeft(Vector.empty[Serialized]) {
          case (acc, n) =>
            val evt = event("p1", n, s"e-$n", bucket)
            val events = acc :+ evt
            ref ! TagWrite(tagName, Vector(evt))
            Thread.sleep(200)
            if (n == 3) {
              probe.within(200.millis) {
                probe.expectMsg(events.map(evt => toEw(evt, evt.sequenceNr)).toVector)
                probe.expectMsg(
                  ProgressWrite("p1", events.last.sequenceNr, events.last.sequenceNr, events.last.timeUuid))
              }
            }
            events
        }

      val remainingFlushedEvents = allEvents.drop(3)
      probe.expectMsg(remainingFlushedEvents.map(evt => toEw(evt, evt.sequenceNr)).toVector)
      probe.expectMsg(
        ProgressWrite(
          "p1",
          remainingFlushedEvents.last.sequenceNr,
          remainingFlushedEvents.last.sequenceNr,
          remainingFlushedEvents.last.timeUuid))

    }

    "flush when time bucket changes" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val bucket = nowBucket()
      val nextBucket = bucket.next()

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(100.millis)

      val e2 = event("p1", 2L, "e-2", nextBucket)
      ref ! TagWrite(tagName, Vector(e2))
      // Buckets are separate partitions, so only send the first
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      val e3 = event("p1", 3L, "e-3", nextBucket)
      val e4 = event("p1", 4L, "e-4", nextBucket)
      ref ! TagWrite(tagName, Vector(e3, e4))
      // batch size has been hit now
      probe.expectMsg(Vector(toEw(e2, 2), toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "flush if time bucket changes within a single msg" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 3))
      val bucket = nowBucket()
      val nextBucket = bucket.next()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", nextBucket)
      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      probe.expectNoMessage(waitDuration)

      val e3 = event("p1", 3L, "e-3", nextBucket)
      val e4 = event("p1", 4L, "e-4", nextBucket)
      ref ! TagWrite(tagName, Vector(e3, e4))
      // batch size has now been hit
      probe.expectMsg(Vector(toEw(e2, 2), toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "not execute query N+1 while query N is outstanding" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2, e3, e4))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "internal flush time buckets one by one if arrive in same msg" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2))
      val now = Uuids.timeBased()
      val bucketOne = TimeBucket(now, bucketSize)
      val bucketTwo = bucketOne.next()
      val bucketThree = bucketTwo.next()

      val e1 = event("p1", 1L, "e-1", bucketOne)
      val e2 = event("p1", 2L, "e-2", bucketTwo)
      val e3 = event("p1", 3L, "e-3", bucketThree)
      val e4 = event("p1", 4L, "e-4", bucketThree)

      ref ! TagWrite(tagName, Vector(e1, e2, e3))
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))

      probe.expectMsg(Vector(toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectNoMessage(waitDuration)
      // fill up batch to flush out the last one
      ref ! TagWrite(tagName, Vector(e4))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "do not internal flush immediately if interval set to 0" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(flushInterval = Duration.Zero))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectMsg(shortDuration, Vector(toEw(e1, 1)))
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid))
      ref ! TagWrite(tagName, Vector(e2))
      probe.expectMsg(shortDuration, Vector(toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
    }

    "do not internal flush if write in progress" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) =
        setup(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      ref ! TagWrite(tagName, Vector(e3, e4))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "do not internal flush if write in progress with no interval" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
        settings = defaultSettings.copy(maxBatchSize = 3, flushInterval = 0.millis))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      ref ! TagWrite(tagName, Vector(e3, e4))
      probe.expectNoMessage(waitDuration)
      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "not flush if internal flush is in progress" in {
      val promiseForWrite = Promise[Done]()
      val (probe, ref) = setup(
        writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
        settings = defaultSettings.copy(maxBatchSize = 2, flushInterval = 500.millis))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(100.millis)
      probe.expectMsg(Vector(toEw(e1, 1)))

      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      ref ! TagWrite(tagName, Vector(e2, e3))
      // should not be written right away and there should not be another flush
      // scheduled
      probe.expectNoMessage(600.millis)

      promiseForWrite.success(Done)
      probe.expectMsg(ProgressWrite("p1", 1, 1, e1.timeUuid)) // from previous write now we've completed the promise
      probe.expectMsg(Vector(toEw(e2, 2), toEw(e3, 3)))
      probe.expectMsg(ProgressWrite("p1", 3, 3, e3.timeUuid))
    }

    "resume from existing sequence nr" in {
      val progress = TagProgress("p1", 100, 10)
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 1))
      val bucket = nowBucket()
      ref ! ResetPersistenceId(tagName, progress)
      expectMsg(ResetPersistenceIdComplete)

      val e1 = event("p1", 101L, "e-1", bucket)
      ref ! TagWrite(tagName, Vector(e1))
      // no first write msg
      probe.expectMsg(Vector(toEw(e1, 11)))
      probe.expectMsg(ProgressWrite("p1", 101, 11, e1.timeUuid))

      val e2 = event("p1", 102L, "e-2", bucket)
      ref ! TagWrite(tagName, Vector(e2))
      probe.expectMsg(Vector(toEw(e2, 12)))
      probe.expectMsg(ProgressWrite("p1", 102, 12, e2.timeUuid))
    }

    "handle timeuuids coming out of order" in {
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 4))
      val currentBucket = (0 to 2).map { _ =>
        val uuid = Uuids.timeBased()
        (uuid, TimeBucket(uuid, bucketSize))
      }

      val futureBucketMillis = Uuids.unixTimestamp(currentBucket(0)._1) + bucketSize.durationMillis
      val futureBucket = TimeBucket(futureBucketMillis, bucketSize)

      val p1e1 = event("p1", 1, "p1-e1", currentBucket(0)._2, uuid = currentBucket(0)._1)
      val p2e1 = event("p2", 1, "p2-e1", futureBucket, uuid = Uuids.startOf(futureBucketMillis))
      val p1e2 = event("p1", 2, "p1-e2", currentBucket(1)._2, uuid = currentBucket(1)._1)

      system.log.debug("Persisting event in bucket: {} uuid: {}", p2e1.timeBucket, formatOffset(p2e1.timeUuid))
      ref ! TagWrite(tagName, Vector(p2e1))
      probe.expectNoMessage(waitDuration)
      system.log.debug(
        "Persisting events in bucket: {} and: {}",
        (p1e1.timeBucket, formatOffset(p1e1.timeUuid)),
        (p1e2.timeBucket, p1e2.timeUuid))
      ref ! TagWrite(tagName, Vector(p1e1, p1e2))
      probe.expectMsg(Vector(toEw(p1e1, 1), toEw(p1e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, p1e2.timeUuid))
    }

    "update expected sequence nr on reset persistence id request" in {
      val pid = "p-1"
      val initialProgress = TagProgress(pid, 10, 10)
      val resetProgress = TagProgress(pid, 5, 5)
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 1))
      val bucket = nowBucket()

      ref ! ResetPersistenceId(tagName, initialProgress)
      expectMsg(ResetPersistenceIdComplete)

      val e11 = event(pid, 11L, "e-11", bucket)
      ref ! TagWrite(tagName, Vector(e11))
      probe.expectMsg(Vector(toEw(e11, 11)))
      probe.expectMsg(ProgressWrite(pid, 11, 11, e11.timeUuid))

      ref ! ResetPersistenceId(tagName, resetProgress)
      expectMsg(ResetPersistenceIdComplete)

      // simulating a restart and recovery starting earlier
      val e6 = event(pid, 6L, "e-6", bucket)
      ref ! TagWrite(tagName, Vector(e6))
      probe.expectMsg(Vector(toEw(e6, 6)))
      probe.expectMsg(ProgressWrite(pid, 6, 6, e6.timeUuid))
    }

    "update expected sequence nr on reset persistence id request (when write in progress)" in {
      val pid = "p-1"
      val initialProgress = TagProgress(pid, 10, 10)
      val resetProgress = TagProgress(pid, 5, 5)
      val writeInProgressPromise = Promise[Done]()
      val (probe, ref) =
        setup(
          settings = defaultSettings.copy(maxBatchSize = 1),
          writeResponse = Stream(writeInProgressPromise.future) ++ Stream.continually(Future.successful(Done)))
      val bucket = nowBucket()

      ref ! ResetPersistenceId(tagName, initialProgress)
      expectMsg(ResetPersistenceIdComplete)
      val e11 = event(pid, 11L, "e-11", bucket)
      ref ! TagWrite(tagName, Vector(e11))
      probe.expectMsg(Vector(toEw(e11, 11)))

      ref ! ResetPersistenceId(tagName, resetProgress)
      expectMsg(ResetPersistenceIdComplete)

      writeInProgressPromise.success(Done)
      probe.expectMsg(ProgressWrite(pid, 11, 11, e11.timeUuid))
      val e6 = event(pid, 6L, "e-6", bucket)
      ref ! TagWrite(tagName, Vector(e6))
      probe.expectMsg(Vector(toEw(e6, 6)))
      probe.expectMsg(ProgressWrite(pid, 6, 6, e6.timeUuid))
    }

    "drop outstanding events for a persistence id when reset" in {
      val pid = "p-1"
      // disable any automatic flushing
      val (probe, ref) = setup(settings = defaultSettings.copy(maxBatchSize = 100, flushInterval = 60.seconds))
      val bucket = nowBucket()

      val e1 = event(pid, 1L, "e-1", bucket)
      val e2 = event(pid, 2L, "e-2", bucket)
      val e3 = event(pid, 3L, "e-3", bucket)
      ref ! TagWrite(tagName, Vector(e1, e2, e3))

      val resetRequest = ResetPersistenceId(tagName, TagProgress(pid, 1, 1))
      ref ! resetRequest
      expectMsg(ResetPersistenceIdComplete)

      // can send 2 and 3 again due to the reset
      ref ! TagWrite(tagName, Vector(e2, e3))

      ref ! Flush
      expectMsg(FlushComplete)
      probe.expectMsg(Vector(toEw(e2, 2), toEw(e3, 3)))
    }

    "forget about a persistence id when idle" in {
      val (probe, underTest) = setup(settings = defaultSettings.copy(maxBatchSize = 1, flushInterval = 60.seconds))
      val pid = "p1"
      val bucket = nowBucket()

      val e1 = event(pid, 1L, "e-1", bucket)
      underTest ! TagWrite(tagName, Vector(e1))
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsgType[ProgressWrite]

      underTest ! DropState("p1")

      val e2 = event(pid, 2L, "e-2", bucket)
      underTest ! TagWrite(tagName, Vector(e2))
      // tag pid sequence nr 1 again as previous was forgotten.
      // this isn't a real scenario tag pid sequence numbers are never re-used however
      // allows testing that the drop state actually did something
      probe.expectMsg(Vector(toEw(e2, 1)))
    }

    "forget about a persistence id when write in progress)" in {
      val pid = "p-1"
      val writeInProgressPromise = Promise[Done]()
      val (probe, underTest) =
        setup(
          settings = defaultSettings.copy(maxBatchSize = 1),
          writeResponse = Stream(writeInProgressPromise.future) ++ Stream.continually(Future.successful(Done)))
      val bucket = nowBucket()

      val e1 = event(pid, 1L, "e-1", bucket)
      underTest ! TagWrite(tagName, Vector(e1))
      underTest ! DropState(pid)

      writeInProgressPromise.success(Done)
      probe.expectMsg(Vector(toEw(e1, 1)))
      probe.expectMsgType[ProgressWrite]

      val e2 = event(pid, 2L, "e-2", bucket)
      underTest ! TagWrite(tagName, Vector(e2))
      // tag pid sequence nr 1 rather than 2
      probe.expectMsg(Vector(toEw(e2, 1)))

    }

    "passivate when idle" in {
      val parent = TestProbe()
      val idleTimeout = 100.millis
      val (probe, ref) =
        setupWithParent(
          settings = defaultSettings.copy(maxBatchSize = 2, stopTagWriterWhenIdle = idleTimeout),
          parent = parent.ref)
      parent.expectMsg(PassivateTagWriter("tag-1"))
      ref.tell(StopTagWriter, parent.ref)
      watch(ref)
      expectTerminated(ref)
    }

    "do not passivate if write in progress" in {
      val promiseForWrite = Promise[Done]()
      val parent = TestProbe()
      val idleTimeout = 1.second
      val (probe, ref) =
        setupWithParent(
          writeResponse = Stream(promiseForWrite.future) ++ Stream.continually(Future.successful(Done)),
          settings = defaultSettings.copy(maxBatchSize = 2, stopTagWriterWhenIdle = idleTimeout),
          parent = parent.ref)
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      parent.expectNoMessage(idleTimeout + 100.millis)

      promiseForWrite.success(Done)
    }

  }

  "Tag writer error scenarios" must {

    "handle tag writes view failing" in {
      val t = TestEx("Tag write failed")
      val (probe, ref) = setup(
        settings = defaultSettings.copy(maxBatchSize = 2),
        writeResponse = Stream(Future.failed(t)) ++ Stream.continually(Future.successful(Done)))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1))
      probe.expectNoMessage(waitDuration)

      ref ! TagWrite(tagName, Vector(e2))
      logProbe.expectMsgPF(waitDuration) {
        case Warning(_, _, msg) if msg.toString.contains("Writing tags has failed") =>
      }
      ref ! TagWrite(tagName, Vector(e3, e4))

      // this one fails
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))

      // should retry next poll
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }

    "handle tag progress write failing" in {
      val t = TestEx("Tag progress write has failed")
      val (probe, ref) =
        setup(
          settings = defaultSettings.copy(maxBatchSize = 2),
          progressWriteResponse = Stream(Future.failed(t)) ++ Stream.continually(Future.successful(Done)))
      val bucket = nowBucket()

      val e1 = event("p1", 1L, "e-1", bucket)
      val e2 = event("p1", 2L, "e-2", bucket)
      val e3 = event("p1", 3L, "e-3", bucket)
      val e4 = event("p1", 4L, "e-4", bucket)

      ref ! TagWrite(tagName, Vector(e1, e2))
      probe.expectMsg(Vector(toEw(e1, 1), toEw(e2, 2)))
      probe.expectMsg(ProgressWrite("p1", 2, 2, e2.timeUuid))
      logProbe.expectMsgPF(waitDuration) {
        case Warning(_, _, msg) if msg.toString.contains("Tag progress write has failed") =>
      }

      ref ! TagWrite(tagName, Vector(e3, e4))
      probe.expectMsg(Vector(toEw(e3, 3), toEw(e4, 4)))
      probe.expectMsg(ProgressWrite("p1", 4, 4, e4.timeUuid))
    }
  }

  private def nowBucket(): TimeBucket = {
    val now = Uuids.timeBased()
    TimeBucket(now, Day)
  }
  private def toEw(s: Serialized, tagPidSequenceNr: Long): EventWrite =
    EventWrite(s.persistenceId, s.sequenceNr, tagPidSequenceNr)

  private def setup(
      tag: String = "tag-1",
      settings: TagWriterSettings,
      writeResponse: Stream[Future[Done]] = Stream.continually(Future.successful(Done)),
      progressWriteResponse: Stream[Future[Done]] = Stream.continually(Future.successful(Done)))
      : (TestProbe, ActorRef) = {
    setupWithParent(tag, settings, writeResponse, progressWriteResponse, TestProbe().ref)
  }

  private def setupWithParent(
      tag: String = "tag-1",
      settings: TagWriterSettings,
      writeResponse: Stream[Future[Done]] = Stream.continually(Future.successful(Done)),
      progressWriteResponse: Stream[Future[Done]] = Stream.continually(Future.successful(Done)),
      parent: ActorRef): (TestProbe, ActorRef) = {
    var writeResponseStream = writeResponse
    var progressWriteResponseStream = progressWriteResponse
    val probe = TestProbe()
    val session =
      new TagWritersSession(null, "unused", "unused", null) {

        override def writeBatch(tag: Tag, events: Seq[(Serialized, Long)])(implicit ec: ExecutionContext) = {
          probe.ref ! events.map {
            case (event, tagPidSequenceNr) => toEw(event, tagPidSequenceNr)
          }
          val (result, tail) = (writeResponseStream.head, writeResponseStream.tail)
          writeResponseStream = tail
          result
        }

        override def writeProgress(
            tag: Tag,
            pid: PersistenceId,
            seqNr: SequenceNr,
            tagPidSequenceNr: TagPidSequenceNr,
            offset: UUID)(implicit ec: ExecutionContext): Future[Done] = {
          val (head, tail) = (progressWriteResponseStream.head, progressWriteResponseStream.tail)
          probe.ref ! ProgressWrite(pid, seqNr, tagPidSequenceNr, offset)
          progressWriteResponseStream = tail
          head
        }
      }

    val ref = system.actorOf(TagWriter.props(settings, session, tag, parent))
    (probe, ref)
  }

  private def event(
      pId: String,
      seqNr: Long,
      payload: String,
      bucket: TimeBucket,
      tags: Set[String] = Set(),
      uuid: UUID = Uuids.timeBased()): Serialized =
    Serialized(pId, seqNr, ByteBuffer.wrap(payload.getBytes()), tags, "", "", 1, "", None, uuid, bucket)

}
