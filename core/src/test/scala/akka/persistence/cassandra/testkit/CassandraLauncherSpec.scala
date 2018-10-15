/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.testkit

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.datastax.driver.core.Cluster
import org.scalatest.{ Matchers, WordSpecLike }
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll

class CassandraLauncherSpec extends TestKit(ActorSystem("CassandraLauncherSpec"))
  with Matchers with WordSpecLike with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    CassandraLauncher.stop()
    super.afterAll()
  }

  private def testCassandra(): Unit = {
    val session =
      Cluster.builder()
        .withClusterName("CassandraLauncherSpec")
        .addContactPoints("localhost").withPort(CassandraLauncher.randomPort)
        .build().connect()
    try
      session.execute("SELECT now() from system.local;").one()
    finally {
      session.close()
      session.getCluster.close()
    }
  }

  "The CassandraLauncher" should {
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
