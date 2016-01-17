/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import scala.concurrent.duration._
import org.scalatest._
import java.io.File
import akka.persistence.cassandra.testkit.CassandraLauncher

trait CassandraLifecycle extends BeforeAndAfterAll { this: Suite =>
  def withSsl: Boolean = false

  def systemName: String

  def cassandraConfigResource: String = CassandraLauncher.DefaultTestConfigResource

  private def toggleSsl(path: String, toggle: Boolean): String =
    if (toggle) path.dropRight(5) + "-ssl" + path.takeRight(5)
    else path

  override protected def beforeAll(): Unit = {
    val cassandraDirectory = new File("target/" + systemName)
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = toggleSsl(cassandraConfigResource, withSsl),
      clean = true,
      port = 0
    )

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }
}
