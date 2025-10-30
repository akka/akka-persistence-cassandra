/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.query

import java.util.UUID

import akka.persistence.PersistentRepr
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory

object EventsByPersistenceIdMultiPartitionGapSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.cassandra.journal.target-partition-size = 15
    akka.persistence.cassandra.query.refresh-interval = 0.5s
    akka.persistence.cassandra.query.events-by-persistence-id-gap-timeout = 4 seconds
    akka.persistence.cassandra.query.gap-free-sequence-numbers = off
    datastax-java-driver.profiles.akka-persistence-cassandra-profile.basic.request.page-size = 2
    akka.stream.materializer.max-input-buffer-size = 4 # there is an async boundary
    """).withFallback(CassandraLifecycle.config)
}

class EventsByPersistenceIdMultiPartitionGapSpec
    extends CassandraSpec(EventsByPersistenceIdMultiPartitionGapSpec.config)
    with DirectWriting {

  "read all events with multi-partition gaps" in {
    val w1 = UUID.randomUUID().toString
    val pr1 = PersistentRepr("e1", 1L, "mpg1", "", writerUuid = w1)
    writeTestEvent(pr1, partitionNr = 0L)

    val pr2 = PersistentRepr("e2", 15L, "mpg1", "", writerUuid = w1)
    writeTestEvent(pr2, partitionNr = 1L)
    deleteTestEvent(pr2, partitionNr = 1L)

    val pr3 = PersistentRepr("e3", 30L, "mpg1", "", writerUuid = w1)
    writeTestEvent(pr3, partitionNr = 2L)
    deleteTestEvent(pr3, partitionNr = 2L)

    val pr4 = PersistentRepr("e4", 45L, "mpg1", "", writerUuid = w1)
    writeTestEvent(pr4, partitionNr = 3L)
    deleteTestEvent(pr4, partitionNr = 3L)

    val pr5 = PersistentRepr("e5", 60L, "mpg1", "", writerUuid = w1)
    writeTestEvent(pr5, partitionNr = 4L)

    val src = queries.currentEventsByPersistenceId("mpg1", 0L, Long.MaxValue)
    val probe = src.map(_.event).runWith(TestSink.probe[Any]).request(10)

    probe.expectNext("e1")
    probe.expectNext("e5")
    probe.cancel()
  }

  "read all events with one-partition gaps" in {
    val w1 = UUID.randomUUID().toString
    val pr1 = PersistentRepr("e1", 1L, "mpg2", "", writerUuid = w1)
    writeTestEvent(pr1, partitionNr = 0L)

    val pr2 = PersistentRepr("e2", 15L, "mpg2", "", writerUuid = w1)
    writeTestEvent(pr2, partitionNr = 1L)

    val pr3 = PersistentRepr("e3", 30L, "mpg2", "", writerUuid = w1)
    writeTestEvent(pr3, partitionNr = 2L)
    deleteTestEvent(pr3, partitionNr = 2L)

    val pr4 = PersistentRepr("e4", 45L, "mpg2", "", writerUuid = w1)
    writeTestEvent(pr4, partitionNr = 3L)

    val pr5 = PersistentRepr("e5", 60L, "mpg2", "", writerUuid = w1)
    writeTestEvent(pr5, partitionNr = 4L)

    val src = queries.currentEventsByPersistenceId("mpg2", 0L, Long.MaxValue)
    val probe = src.map(_.event).runWith(TestSink.probe[Any]).request(10)

    probe.expectNext("e1")
    probe.expectNext("e2")
    probe.expectNext("e4")
    probe.expectNext("e5")
    probe.cancel()
  }

  "read all events with no partition gaps" in {
    val w1 = UUID.randomUUID().toString
    val pr1 = PersistentRepr("e1", 1L, "mpg3", "", writerUuid = w1)
    writeTestEvent(pr1, partitionNr = 0L)

    val pr2 = PersistentRepr("e2", 15L, "mpg3", "", writerUuid = w1)
    writeTestEvent(pr2, partitionNr = 1L)

    val pr3 = PersistentRepr("e3", 30L, "mpg3", "", writerUuid = w1)
    writeTestEvent(pr3, partitionNr = 2L)

    val pr4 = PersistentRepr("e4", 45L, "mpg3", "", writerUuid = w1)
    writeTestEvent(pr4, partitionNr = 3L)

    val pr5 = PersistentRepr("e5", 60L, "mpg3", "", writerUuid = w1)
    writeTestEvent(pr5, partitionNr = 4L)

    val src = queries.currentEventsByPersistenceId("mpg3", 0L, Long.MaxValue)
    val probe = src.map(_.event).runWith(TestSink.probe[Any]).request(10)

    probe.expectNext("e1")
    probe.expectNext("e2")
    probe.expectNext("e3")
    probe.expectNext("e4")
    probe.expectNext("e5")
    probe.cancel()
  }

  "read all events with in & out partition gaps " in {
    val w1 = UUID.randomUUID().toString
    val pr1 = PersistentRepr("e1", 5L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr1, partitionNr = 0L)

    val pr2 = PersistentRepr("e2", 10L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr2, partitionNr = 0L)

    val pr3 = PersistentRepr("e3", 30L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr3, partitionNr = 1L)
    deleteTestEvent(pr3, partitionNr = 1L)

    val pr4 = PersistentRepr("e4", 47L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr4, partitionNr = 2L)

    val pr5 = PersistentRepr("e5", 50L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr5, partitionNr = 3L)

    val pr6 = PersistentRepr("e6", 55L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr6, partitionNr = 3L)

    val pr7 = PersistentRepr("e7", 60L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr7, partitionNr = 4L)
    deleteTestEvent(pr7, partitionNr = 4L)

    val pr8 = PersistentRepr("e8", 75L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr8, partitionNr = 5L)
    deleteTestEvent(pr8, partitionNr = 5L)

    val pr9 = PersistentRepr("e9", 90L, "mpg4", "", writerUuid = w1)
    writeTestEvent(pr9, partitionNr = 6L)

    val src = queries.currentEventsByPersistenceId("mpg4", 0L, Long.MaxValue)
    val probe = src.map(_.event).runWith(TestSink.probe[Any]).request(10)

    probe.expectNext("e1")
    probe.expectNext("e2")
    probe.expectNext("e4")
    probe.expectNext("e5")
    probe.expectNext("e6")
    probe.expectNext("e9")
    probe.cancel()
  }

  "read all events with empty first partitions" in {
    val w1 = UUID.randomUUID().toString

    val pr1 = PersistentRepr("e1", 5L, "mpg5", "", writerUuid = w1)
    writeTestEvent(pr1, partitionNr = 0L)
    deleteTestEvent(pr1, partitionNr = 0L)

    val pr2 = PersistentRepr("e2", 15L, "mpg5", "", writerUuid = w1)
    writeTestEvent(pr2, partitionNr = 1L)
    deleteTestEvent(pr2, partitionNr = 1L)

    val pr3 = PersistentRepr("e3", 47L, "mpg5", "", writerUuid = w1)
    writeTestEvent(pr3, partitionNr = 2L)

    val pr4 = PersistentRepr("e4", 50L, "mpg5", "", writerUuid = w1)
    writeTestEvent(pr4, partitionNr = 3L)

    val src = queries.currentEventsByPersistenceId("mpg5", 0L, Long.MaxValue)
    val probe = src.map(_.event).runWith(TestSink.probe[Any]).request(10)

    probe.expectNext("e3")
    probe.expectNext("e4")
    probe.cancel()
  }
}
