/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraLifecycle.AwaitPersistenceInit
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._

object ReconnectSpec {
  val config = ConfigFactory.parseString(
    s"""
      |cassandra-journal.keyspace=ReconnectSpec
      |cassandra-snapshot-store.keyspace=ReconnectSpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

}

class ReconnectSpec extends TestKit(ActorSystem("ReconnectSpec", ReconnectSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  override def systemName: String = "ReconnectSpec"

  // important, since we are testing the initialization and starting Cassandra later
  override protected def beforeAll(): Unit = ()

  "Journal" should {

    "reconnect if Cassandra is not started" in {
      val a1 = system.actorOf(Props(classOf[AwaitPersistenceInit], "", ""))
      watch(a1)
      a1 ! "hello"
      expectTerminated(a1, 10.seconds)

      startCassandra()
      awaitPersistenceInit()
    }
  }

}
