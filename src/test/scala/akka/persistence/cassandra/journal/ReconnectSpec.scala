/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.persistence.cassandra.testkit.CassandraLauncher
import java.util.UUID
import scala.concurrent.duration._
import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._
import java.io.File
import akka.persistence.cassandra.CassandraLifecycle.AwaitPersistenceInit

object ReconnectSpec {
  val config = ConfigFactory.parseString(
    s"""
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
      |cassandra-journal.keyspace=ReconnectSpec
      |cassandra-snapshot-store.keyspace=ReconnectSpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

}

class ReconnectSpec extends TestKit(ActorSystem("ReconnectSpec", ReconnectSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  import ReconnectSpec._

  override def systemName: String = "ReconnectSpec"

  // important, since we are testing the initialization and starting Cassandra later
  override protected def beforeAll(): Unit = ()

  "Journal" should {

    "reconnect if Cassandra is not started" in {
      val a1 = system.actorOf(Props[AwaitPersistenceInit])
      watch(a1)
      a1 ! ("hello")
      expectTerminated(a1, 10.seconds)

      startCassandra()
      awaitPersistenceInit()
    }
  }

}
