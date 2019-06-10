/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
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
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-query-journal.events-by-persistence-id-gap-timeout = 4 seconds
    cassandra-journal.target-partition-size = 15
    cassandra-query-journal.gap-free-sequence-numbers = off
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
}
