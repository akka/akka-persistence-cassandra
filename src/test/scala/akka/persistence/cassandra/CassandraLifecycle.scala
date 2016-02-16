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

object CassandraLifecycle {

  val config = ConfigFactory.parseString("""
    akka.persistence.journal.plugin = "cassandra-journal"
    akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
    cassandra-journal.circuit-breaker.call-timeout = 30s
    akka.test.single-expect-default = 20s
    akka.actor.serialize-messages=on
    """)

  def awaitPersistenceInit(system: ActorSystem): Unit = {
    val probe = TestProbe()(system)
    system.actorOf(Props[AwaitPersistenceInit]).tell("hello", probe.ref)
    probe.expectMsg(35.seconds, "hello")
  }

  class AwaitPersistenceInit extends PersistentActor {
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
    val cassandraDirectory = new File("target/" + systemName)
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = cassandraConfigResource,
      clean = true,
      port = 0
    )

    awaitPersistenceInit()
    super.beforeAll()
  }

  def awaitPersistenceInit(): Unit = {
    CassandraLifecycle.awaitPersistenceInit(system)
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }

}
