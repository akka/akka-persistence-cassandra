/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import scala.concurrent.duration._

import org.scalatest._

import akka.persistence.cassandra.server.CassandraServer

trait CassandraLifecycle extends BeforeAndAfterAll { this: Suite =>
  def withSsl: Boolean = false

  override protected def beforeAll(): Unit = {
    CassandraServer.start(60.seconds, withSsl)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    CassandraServer.clean()
    super.afterAll()
  }
}
