/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ ActorSystem, Props }
import akka.persistence.PersistentActor
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.{ TestKitBase, TestProbe }
import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object CassandraLifecycle {
  sealed trait CassandraMode
  final case object Embedded extends CassandraMode
  final case object External extends CassandraMode

  // Set to external to use your own cassandra instance running on localhost:9042
  // beware that most tests rely on the data directory being removed for clean up
  // which won't happen for an external cassandra unless extending CassandraSpec
  //  val mode: CassandraMode = Embedded
  val mode: CassandraMode = Option(System.getenv("CASSANDRA_MODE")).map(_.toLowerCase) match {
    case Some("external") => External
    case Some("embedded") => Embedded
    case _                => External
  }

  def isExternal: Boolean = mode == External

  val config = {
    val always = ConfigFactory.parseString(s"""
    akka.test.timefactor = $${?AKKA_TEST_TIMEFACTOR}
    akka.persistence.journal.plugin = "cassandra-journal"
    akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
    cassandra-journal.circuit-breaker.call-timeout = 30s
    akka.test.single-expect-default = 20s
    akka.test.filter-leeway = 20s
    akka.actor.serialize-messages=on
    """).resolve()

    // this isn't used if extending CassandraSpec
    val port = mode match {
      case Embedded =>
        CassandraLauncher.randomPort
      case External =>
        9042
    }

    always.withFallback(ConfigFactory.parseString(s"""
      cassandra-journal.port = $port
      cassandra-snapshot-store.port = $port
    """))
  }

  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String = "", snapshotPluginId: String = ""): Unit = {
    val probe = TestProbe()(system)
    val t0 = System.nanoTime()
    var n = 0
    probe.within(45.seconds) {
      probe.awaitAssert {
        n += 1
        system
          .actorOf(Props(classOf[AwaitPersistenceInit], journalPluginId, snapshotPluginId), "persistenceInit" + n)
          .tell("hello", probe.ref)
        probe.expectMsg(5.seconds, "hello")
        system.log.debug(
          "awaitPersistenceInit took {} ms {}",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0),
          system.name)
      }
    }
  }

  class AwaitPersistenceInit(override val journalPluginId: String, override val snapshotPluginId: String)
      extends PersistentActor {
    def persistenceId: String = "persistenceInit"

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

  import CassandraLifecycle._

  def systemName: String

  def cassandraConfigResource: String = CassandraLauncher.DefaultTestConfigResource

  lazy val cluster = {
    Cluster
      .builder()
      .addContactPoint("localhost")
      .withClusterName(systemName + "TestCluster")
      .withPort(system.settings.config.getInt("cassandra-journal.port"))
      .build()
  }

  override protected def beforeAll(): Unit = {
    startCassandra(port())
    awaitPersistenceInit()
    super.beforeAll()
  }

  def port(): Int = 0

  def startCassandra(): Unit =
    startCassandra(port())

  def startCassandra(port: Int): Unit =
    mode match {
      case Embedded =>
        val cassandraDirectory = new File("target/" + systemName)
        CassandraLauncher.start(
          cassandraDirectory,
          configResource = cassandraConfigResource,
          clean = true,
          port = port,
          CassandraLauncher.classpathForResources("logback-test.xml"))
      case External =>
    }

  def awaitPersistenceInit(): Unit =
    CassandraLifecycle.awaitPersistenceInit(system)

  override protected def afterAll(): Unit =
    try {
      shutdown(system, verifySystemShutdown = true)
    } finally {
      mode match {
        case Embedded =>
          CassandraLauncher.stop()
        case External =>
          externalCassandraCleanup()
      }
      super.afterAll()
    }

  def dropKeyspaces(): Unit = {
    val journalKeyspace = system.settings.config.getString("cassandra-journal.keyspace")
    val snapshotKeyspace = system.settings.config.getString("cassandra-snapshot-store.keyspace")
    val dropped = Try {
      cluster.connect().execute(s"drop keyspace if exists ${journalKeyspace}")
      cluster.connect().execute(s"drop keyspace if exists ${snapshotKeyspace}")
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
  protected def externalCassandraCleanup(): Unit = dropKeyspaces()
}
