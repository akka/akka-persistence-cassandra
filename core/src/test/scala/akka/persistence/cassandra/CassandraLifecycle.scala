/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.net.InetSocketAddress
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import akka.actor.{ ActorSystem, PoisonPill, Props }
import akka.persistence.PersistentActor
import akka.testkit.{ TestKitBase, TestProbe }
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.scalatest._
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object CassandraLifecycle {

  val firstTimeBucket: String = {
    val today = LocalDateTime.now(ZoneOffset.UTC)
    val firstBucketFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")
    today.minusMinutes(5).format(firstBucketFormat)
  }

  val config =
    ConfigFactory.parseString(s"""
    akka.test.timefactor = $${?AKKA_TEST_TIMEFACTOR}
    akka.persistence.journal.plugin = "cassandra-plugin.journal"
    akka.persistence.snapshot-store.plugin = "cassandra-plugin.snapshot"
    cassandra-plugin.journal.circuit-breaker.call-timeout = 30s
    cassandra-plugin.query.first-time-bucket = "$firstTimeBucket"
    akka.test.single-expect-default = 20s
    akka.test.filter-leeway = 20s
    akka.actor.serialize-messages=on
    # needed when testing with Akka 2.6
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """).resolve()

  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    val probe = TestProbe()(system)
    val t0 = System.nanoTime()
    var n = 0
    probe.within(45.seconds) {
      probe.awaitAssert(
        {
          n += 1
          val a =
            system.actorOf(
              Props(classOf[AwaitPersistenceInit], "persistenceInit" + n, journalPluginId, snapshotPluginId),
              "persistenceInit" + n)
          a.tell("hello", probe.ref)
          try {
            probe.expectMsg(5.seconds, "hello")
          } catch {
            case t: Throwable =>
              probe.watch(a)
              a ! PoisonPill
              probe.expectTerminated(a, 10.seconds)
              throw t
          }
          system.log.debug(
            "awaitPersistenceInit took {} ms {}",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0),
            system.name)
        },
        probe.remainingOrDefault,
        2.seconds)
    }
  }

  class AwaitPersistenceInit(
      override val persistenceId: String,
      override val journalPluginId: String,
      override val snapshotPluginId: String)
      extends PersistentActor {

    def receiveRecover: Receive = {
      case _ =>
    }

    def receiveCommand: Receive = {
      case msg =>
        persist(msg) { _ =>
          sender() ! msg
          context.stop(self)
        }
    }
  }
}

trait CassandraLifecycle extends BeforeAndAfterAll with TestKitBase {
  this: Suite =>

  def systemName: String

  def port(): Int = 9042

  lazy val cluster = {
    CqlSession
      .builder()
      .withLocalDatacenter("datacenter1")
      .addContactPoint(new InetSocketAddress("localhost", port()))
      .build()
  }

  override protected def beforeAll(): Unit = {
    awaitPersistenceInit()
    super.beforeAll()
  }

  def awaitPersistenceInit(): Unit = {
    CassandraLifecycle.awaitPersistenceInit(system)
  }

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    externalCassandraCleanup()
    super.afterAll()
  }

  def dropKeyspaces(): Unit = {
    val journalKeyspace = system.settings.config.getString("cassandra-plugin.journal.keyspace")
    val snapshotKeyspace = system.settings.config.getString("cassandra-plugin.snapshot.keyspace")
    val dropped = Try {
      cluster.execute(s"drop keyspace if exists ${journalKeyspace}")
      cluster.execute(s"drop keyspace if exists ${snapshotKeyspace}")
    }
    dropped match {
      case Failure(t) => system.log.error(t, "Failed to drop keyspaces {} {}", journalKeyspace, snapshotKeyspace)
      case Success(_) =>
    }
  }

  /**
   * Only called if using an external cassandra. Override to clean up
   * keyspace etc. Defaults to dropping the keyspaces.
   */
  protected def externalCassandraCleanup(): Unit = {
    dropKeyspaces()
  }
}
