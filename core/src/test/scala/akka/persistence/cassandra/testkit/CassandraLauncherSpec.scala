/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.testkit

import java.io.File
import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.datastax.oss.driver.api.core.CqlSession
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll

class CassandraLauncherSpec
    extends TestKit(ActorSystem("CassandraLauncherSpec"))
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    CassandraLauncher.stop()
    super.afterAll()
  }

  private def testCassandra(): Unit = {
    val session =
      CqlSession.builder().addContactPoint(new InetSocketAddress("localhost", CassandraLauncher.randomPort)).build()
    try session.execute("SELECT now() from system.local;").one()
    finally {
      session.close()
    }
  }

  "The CassandraLauncher" must {
    "support forking" in {
      val cassandraDirectory = new File("target/" + system.name)
      CassandraLauncher.start(
        cassandraDirectory,
        configResource = CassandraLauncher.DefaultTestConfigResource,
        clean = true,
        port = 0,
        CassandraLauncher.classpathForResources("logback-test.xml"))

      awaitAssert({
        testCassandra()
      }, 45.seconds)

      CassandraLauncher.stop()

      an[Exception] shouldBe thrownBy(testCassandra())
    }
  }

}
