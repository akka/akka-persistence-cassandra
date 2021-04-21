/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import akka.actor.{ ActorSystem, PoisonPill, Props }
import akka.persistence.PersistentActor
import akka.persistence.cassandra.CassandraLifecycle.journalTables
import akka.persistence.cassandra.CassandraLifecycle.snapshotTables
import akka.testkit.{ TestKitBase, TestProbe }
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata

import java.util

object CassandraLifecycle {

  val firstTimeBucket: String = {
    val today = LocalDateTime.now(ZoneOffset.UTC)
    val firstBucketFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")
    today.minusMinutes(5).format(firstBucketFormat)
  }

  val config =
    ConfigFactory.parseString(s"""
    akka.test.timefactor = $${?AKKA_TEST_TIMEFACTOR}
    akka.persistence.journal.plugin = "akka.persistence.cassandra.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.cassandra.snapshot"
    akka.persistence.cassandra.journal.circuit-breaker.call-timeout = 30s
    akka.persistence.cassandra.journal.max-failures = 20
    akka.persistence.cassandra.snapshot.max-failures = 20
    akka.persistence.cassandra.events-by-tag.first-time-bucket = "$firstTimeBucket"
    akka.test.single-expect-default = 20s
    akka.test.filter-leeway = 20s
    akka.actor.serialize-messages=on
    # needed when testing with Akka 2.6
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    akka.use-slf4j = off
    
    datastax-java-driver { 
      basic.contact-points = [ "cassandra.eu-central-1.amazonaws.com:9142"]
      basic.request.consistency = LOCAL_QUORUM
      basic.load-balancing-policy {
        class = DefaultLoadBalancingPolicy
        local-datacenter = eu-central-1
      }
      advanced {
        auth-provider = {
          class = software.aws.mcs.auth.SigV4AuthProvider
          aws-region = eu-central-1
        }
        ssl-engine-factory {
          class = DefaultSslEngineFactory
        }
      }
    }
    
    """).withFallback(CassandraSpec.enableAutocreate).resolve()

  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    // Order matters!!
    // awaitPersistencePluginInit will do a best effort attempt to wait for the plugin to be
    // ready and trigger the creation of tables.
    awaitPersistencePluginInit(system, journalPluginId, snapshotPluginId)
    // awaitTablesCreated is a hard wait ensuring tables were indeed created
    awaitTablesCreated(system, journalPluginId, snapshotPluginId)
  }

  val journalTables =
    Set("all_persistence_ids", "messages", "metadata", "tag_scanning", "tag_views", "tag_write_progress")
  val snapshotTables = Set("snapshots")

  def awaitTablesCreated(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    val journalName =
      if (journalPluginId == "")
        system.settings.config.getString("akka.persistence.cassandra.journal.keyspace")
      else journalPluginId
    val snapshotName =
      if (snapshotPluginId == "")
        system.settings.config.getString("akka.persistence.cassandra.snapshot.keyspace")
      else snapshotPluginId
    lazy val cluster: CqlSession =
      Await.result(session.underlying(), 10.seconds)

    def session: CassandraSession = {
      CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")
    }
    awaitTableCount(cluster, 45, journalName, actual => journalTables.subsetOf(actual.toSet))
    awaitTableCount(cluster, 45, snapshotName, actual => snapshotTables.subsetOf(actual.toSet))
  }

  def awaitTableCount(
      cluster: CqlSession,
      retries: Int,
      keyspaceName: String,
      readinessCheck: Array[String] => Boolean): Unit = {
    val tables: util.Map[CqlIdentifier, TableMetadata] =
      cluster.getMetadata.getKeyspace(keyspaceName).get().getTables

    val tableNames = tables.keySet().toArray.map(_.toString)

    if (retries == 0) {
      println(s"Awaiting table creation/drop timed out. Current list of tables:  [${tableNames.mkString(", ")}]")
      throw new RuntimeException("Awaiting table creation/drop timed out.")
    } else if (readinessCheck(tableNames)) {
      ()
    } else {
      println(s"Awaiting table creation/deletion. Existing tables: [${tableNames.mkString(", ")}].")
      Thread.sleep(1000)
      awaitTableCount(cluster, retries - 1, keyspaceName, readinessCheck)
    }
  }

  private def awaitPersistencePluginInit(
      system: ActorSystem,
      journalPluginId: String = "",
      snapshotPluginId: String = ""): Unit = {
    val probe = TestProbe()(system)
    val t0 = System.nanoTime()
    var n = 0
    probe.within(75.seconds) {
      probe.awaitAssert(
        {
          n += 1
          val a =
            system.actorOf(
              Props(classOf[AwaitPersistenceInit], "persistenceInit" + n, journalPluginId, snapshotPluginId),
              "persistenceInit" + n)
          a.tell("hello", probe.ref)
          try {
            probe.expectMsg(10.seconds, "hello")
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

  lazy val cluster: CqlSession =
    Await.result(session.underlying(), 10.seconds)

  def session: CassandraSession = {
    CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")
  }

  override protected def beforeAll(): Unit = {
    try {
      awaitPersistenceInit()
    } catch {
      case NonFatal(e) =>
//        Try(externalCassandraCleanup())
        shutdown(system, verifySystemShutdown = false)
        throw e
    }
    super.beforeAll()
  }

  private var initOk = false

  def awaitPersistenceInit(): Unit = {
    CassandraLifecycle.awaitPersistenceInit(system)
    initOk = true
  }

  override protected def afterAll(): Unit = {
    if (initOk)
      externalCassandraCleanup()
    shutdown(system, verifySystemShutdown = true)
    super.afterAll()
  }

  def dropKeyspaces(): Unit = {
    val journalKeyspace = "ignasi20210419002" // FIXME system.settings.config.getString("akka.persistence.cassandra.journal.keyspace")
    val snapshotKeyspace = "ignasi20210419002" // FIXME system.settings.config.getString("akka.persistence.cassandra.snapshot.keyspace")
    val dropped = Try {

      println("  -------------------  DROPPING TABLES....")

      cluster.execute(s"drop table if exists ${journalKeyspace}.all_persistence_ids")
      cluster.execute(s"drop table if exists ${journalKeyspace}.messages")
      cluster.execute(s"drop table if exists ${journalKeyspace}.metadata")
      cluster.execute(s"drop table if exists ${journalKeyspace}.tag_scanning")
      cluster.execute(s"drop table if exists ${journalKeyspace}.tag_views")
      cluster.execute(s"drop table if exists ${journalKeyspace}.tag_write_progress")

      cluster.execute(s"drop table if exists ${snapshotKeyspace}.snapshots")

      CassandraLifecycle.awaitTableCount(
        cluster,
        45,
        journalKeyspace,
        actual => actual.toSet.intersect(journalTables).isEmpty)
      CassandraLifecycle.awaitTableCount(
        cluster,
        45,
        snapshotKeyspace,
        actual => actual.toSet.intersect(snapshotTables).isEmpty)

      //cluster.execute(s"drop keyspace if exists ${journalKeyspace}")
      //cluster.execute(s"drop keyspace if exists ${snapshotKeyspace}")
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
