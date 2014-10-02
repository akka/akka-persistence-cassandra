package akka.persistence.cassandra

import scala.concurrent.duration._

import org.scalatest._

import akka.persistence.cassandra.server.CassandraServer

trait CassandraLifecycle extends BeforeAndAfterAll { this: Suite =>
  override protected def beforeAll(): Unit = {
    CassandraServer.start(60.seconds)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    CassandraServer.clean()
    super.afterAll()
  }
}
