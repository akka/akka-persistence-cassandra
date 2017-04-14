/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.io.File
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.TestKitBase
import akka.testkit.TestProbe
import org.scalatest._
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit

object CassandraLifecycle {

  val config = ConfigFactory.parseString(s"""
    akka.persistence.journal.plugin = "cassandra-journal"
    akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
    cassandra-journal.port = ${CassandraLauncher.randomPort}
    cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
    cassandra-journal.circuit-breaker.call-timeout = 30s
    akka.test.single-expect-default = 20s
    akka.actor.serialize-messages=on
    """)

  def awaitPersistenceInit(system: ActorSystem, journalPluginId: String="", snapshotPluginId: String=""): Unit = {
    val probe = TestProbe()(system)
    val t0 = System.nanoTime()
    var n = 0
    probe.within(45.seconds) {
      probe.awaitAssert {
        n += 1
        system.actorOf(Props(classOf[AwaitPersistenceInit], journalPluginId, snapshotPluginId), "persistenceInit" + n).tell("hello", probe.ref)
        probe.expectMsg(5.seconds, "hello")
        system.log.debug("awaitPersistenceInit took {} ms {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0), system.name)
      }
    }
  }

  class AwaitPersistenceInit(
    override val journalPluginId:  String,
    override val snapshotPluginId: String
  ) extends PersistentActor {
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

trait CassandraLifecycle extends BeforeAndAfterAll { this: TestKitBase with Suite =>

  def systemName: String

  def cassandraConfigResource: String = CassandraLauncher.DefaultTestConfigResource

  override protected def beforeAll(): Unit = {
    startCassandra()
    awaitPersistenceInit()
    super.beforeAll()
  }

  def startCassandra(): Unit = {
    val cassandraDirectory = new File("target/" + systemName)
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = cassandraConfigResource,
      clean = true,
      port = 0
    )
  }

  def awaitPersistenceInit(): Unit = {
    CassandraLifecycle.awaitPersistenceInit(system)
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }

}
