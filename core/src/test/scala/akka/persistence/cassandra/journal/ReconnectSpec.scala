/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence.cassandra.CassandraLifecycle.AwaitPersistenceInit
import akka.persistence.cassandra.CassandraSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object ReconnectSpec {
  val config = ConfigFactory.parseString(
    s"""
      |cassandra-journal.keyspace=ReconnectSpec
      |cassandra-snapshot-store.keyspace=ReconnectSpecSnapshot
    """
  )
}

class ReconnectSpec extends CassandraSpec(ReconnectSpec.config) {

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
