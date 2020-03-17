/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object CassandraConfiguration {
  val customQueryConfig: Config = ConfigFactory.parseString(s"""
       akka.persistence.cassandra.query.health-check-query="SELECT * FROM system.peers"
    """)
}

class AkkaPersistenceCassandraHealthCheckDefaultQuerySpec extends CassandraSpec with CassandraLifecycle {

  "AkkaPersistenceCassandraHealthCheckDefaultQuerySpec" must {
    "reply with successful health check result when plugin uses default query" in {
      val healthCheckFuture = new AkkaPersistenceCassandraHealthCheck(system)()

      val healthCheckResult = Await.result(healthCheckFuture, 1 second)

      healthCheckResult shouldBe true
    }
  }
}

class AkkaPersistenceCassandraHealthCheckCustomQuerySpec extends CassandraSpec(s"""
       akka.persistence.cassandra.query.health-check-query="SELECT * FROM system.peers"
    """) with CassandraLifecycle {

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster.execute("INSERT INTO system.peers(peer, data_center) VALUES ('10.0.0.1', 'cassandra-dc')")
    cluster.execute("INSERT INTO system.peers(peer, data_center) VALUES ('10.0.0.2', 'cassandra-dc')")
  }

  override def afterAll(): Unit = {
    cluster.execute("DELETE FROM system.peers WHERE peer = '10.0.0.1'")
    cluster.execute("DELETE FROM system.peers WHERE peer = '10.0.0.2'")
    super.afterAll()
  }

  "AkkaPersistenceCassandraHealthCheckCustomQuerySpec" must {
    "reply with successful health check result when plugin executes custom query and result is non-empty" in {
      val healthCheckFuture = new AkkaPersistenceCassandraHealthCheck(system)()

      val healthCheckResult = Await.result(healthCheckFuture, 1 second)

      healthCheckResult shouldBe true
    }
  }
}

class AkkaPersistenceCassandraHealthCheckCustomFailingQuerySpec
    extends CassandraSpec(CassandraConfiguration.customQueryConfig)
    with CassandraLifecycle {

  "AkkaPersistenceCassandraHealthCheckCustomFailingQuerySpec" must {
    "reply with failed health check result when plugin executes custom query and result is empty" in {
      val healthCheckFuture = new AkkaPersistenceCassandraHealthCheck(system)()

      val healthCheckResult = Await.result(healthCheckFuture, 1 second)

      healthCheckResult shouldBe false
    }
  }
}
