/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.io.File

import akka.actor.{ ActorSystem, Props }
import akka.persistence.cassandra.CassandraLifecycle.AwaitPersistenceInit
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.{ ImplicitSender, SocketUtil, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.Suite
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object ReconnectSpec {
  val freePort = SocketUtil.temporaryLocalPort()
  val config = ConfigFactory
    .parseString(s"""
      datastax-java-driver {
        basic.load-balancing-policy.local-datacenter = "datacenter1"
        // Will fail without this setting 
        advanced.reconnect-on-init = true      
        basic.contact-points = ["127.0.0.1:$freePort"]
      }
      """)
    .withFallback(CassandraLifecycle.config)
}

// not using Cassandra Spec
class ReconnectSpec
    extends TestKit(ActorSystem("ReconnectSpec", ReconnectSpec.config))
    with Suite
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures {

  "Reconnecting" must {
    "start with system off" in {
      val pa = system.actorOf(Props(new AwaitPersistenceInit("pid", "", "")))
      pa ! "hello"
      expectNoMessage()

      CassandraLauncher.start(
        new File("target/ReconnectSpec"),
        configResource = CassandraLauncher.DefaultTestConfigResource,
        clean = true,
        port = ReconnectSpec.freePort,
        CassandraLauncher.classpathForResources("logback-test.xml"))

      try {
        CassandraLifecycle.awaitPersistenceInit(system)
      } finally {
        CassandraLauncher.stop()
      }

    }
  }

}
