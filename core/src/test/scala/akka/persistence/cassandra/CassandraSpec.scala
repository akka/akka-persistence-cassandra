/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.{ LocalDateTime, ZoneOffset }
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraLifecycle.{ Embedded, External }
import akka.persistence.cassandra.CassandraSpec._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ NoOffset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, SocketUtil, TestKitBase }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.{ Matchers, WordSpecLike }

import scala.collection.immutable
import scala.concurrent.duration._
import akka.persistence.cassandra.journal.CassandraJournal
import akka.serialization.SerializationExtension
import com.datastax.driver.core.Cluster

import scala.util.Try

object CassandraSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  def getCallerName(clazz: Class[_]): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1)
      .dropWhile(_ matches "(java.lang.Thread|.*Abstract.*)")
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 ⇒ s
      case z  ⇒ s drop (z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def configOverrides(journalKeyspace: String, snapshotStoreKeyspace: String, port: Int): Config = ConfigFactory.parseString(
    s"""
      cassandra-journal {
        keyspace = $journalKeyspace
        port = $port
      }

      cassandra-snapshot-store {
        keyspace = $snapshotStoreKeyspace
        port = $port
      }
    """
  )

  val fallbackConfig = ConfigFactory.parseString(
    s"""
      first-time-bucket = "${today.minusHours(2).format(query.firstBucketFormat)}"
    """
  )

}

/**
 * Picks a free port for Cassandra before starting the ActorSystem
 */
abstract class CassandraSpec(config: Config, val journalName: String = getCallerName(getClass), val snapshotName: String = getCallerName(getClass)) extends TestKitBase
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  def this(config: String) = this(ConfigFactory.parseString(config))

  lazy val randomPort = SocketUtil.temporaryLocalPort()

  override def port(): Int = CassandraLifecycle.mode match {
    case External => 9042
    case Embedded => randomPort
  }

  override protected def externalCassandraCleanup(): Unit = {
    Try {
      system.log.info(s"Dropping keysapces: $journalName $snapshotName")
      cluster.connect().execute(s"drop keyspace if exists $journalName")
      cluster.connect().execute(s"drop keyspace if exists $snapshotName")
      cluster.close()
    }
  }

  final implicit lazy val system: ActorSystem = {
    // always use this port and keyspace generated here, then test config, then the lifecycle config
    val finalConfig =
      configOverrides(journalName, snapshotName, port())
        .withFallback(config) // test's config
        .withFallback(fallbackConfig) // generally good config that tests can override
        .withFallback(CassandraLifecycle.config)
        .withFallback(ConfigFactory.load())
        .resolve()

    val as = ActorSystem(journalName, finalConfig)
    as.log.info("Using key spaces: {} {}", journalName, snapshotName)
    as
  }

  final override lazy val cluster = Cluster.builder()
    .addContactPoint("localhost")
    .withPort(port())
    .build()

  final override def systemName = system.name

  implicit val mat = ActorMaterializer()(system)

  implicit val patience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Milliseconds))

  val pidCounter = new AtomicInteger()
  def nextPid = s"pid=${pidCounter.incrementAndGet()}"

  val eventDeserializer: CassandraJournal.EventDeserializer = new CassandraJournal.EventDeserializer(system)

  lazy val queries: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def eventsPayloads(pid: String): Seq[Any] =
    queries.currentEventsByPersistenceId(pid, 0, Long.MaxValue)
      .map(e => e.event)
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

  def events(pid: String): immutable.Seq[EventsByPersistenceIdStage.TaggedPersistentRepr] =
    queries.eventsByPersistenceId(pid, 0, Long.MaxValue, Long.MaxValue, 100, None, "test",
      extractor = Extractors.taggedPersistentRepr(eventDeserializer, SerializationExtension(system)))
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

  def eventPayloadsWithTags(pid: String): immutable.Seq[(Any, Set[String])] =
    queries.eventsByPersistenceId(pid, 0, Long.MaxValue, Long.MaxValue, 100, None, "test",
      extractor = Extractors.taggedPersistentRepr(eventDeserializer, SerializationExtension(system)))
      .map { tpr => (tpr.pr.payload, tpr.tags) }
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

  def eventsByTag(tag: String): TestSubscriber.Probe[Any] = {
    queries.eventsByTag(tag, NoOffset)
      .map(_.event)
      .runWith(TestSink.probe)
  }

  def expectEventsForTag(tag: String, elements: String*): Unit = {
    val probe = queries.eventsByTag(tag, NoOffset)
      .map(_.event)
      .runWith(TestSink.probe)

    probe.request(elements.length + 1)
    elements.foreach(probe.expectNext)
    probe.expectNoMessage(10.millis)
    probe.cancel()
  }
}
