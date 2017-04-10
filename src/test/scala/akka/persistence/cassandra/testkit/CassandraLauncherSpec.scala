/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.testkit

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.datastax.driver.core.Cluster
import org.scalatest.{ Matchers, WordSpecLike }
import scala.concurrent.duration._

class CassandraLauncherSpec extends TestKit(ActorSystem("CassandraLauncherSpec")) with Matchers with WordSpecLike {

  private def testCassandra() = {
    Cluster.builder()
      .addContactPoints("localhost").withPort(CassandraLauncher.randomPort)
      .build().connect().execute("SELECT now() from system.local;").one()
  }

  "The CassandraLauncher" should {
    "support forking" in {
      val cassandraDirectory = new File("target/" + system.name)
      CassandraLauncher.start(
        cassandraDirectory,
        configResource = CassandraLauncher.DefaultTestConfigResource,
        clean = true,
        port = 0,
        fork = true
      )

      awaitAssert({
        testCassandra()
      }, 45.seconds)

      CassandraLauncher.stop()

      an[Exception] shouldBe thrownBy(testCassandra())
    }
  }

}
