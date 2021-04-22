/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.healthcheck

import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Milliseconds
import org.scalatest.time.Seconds
import org.scalatest.time.Span

class CassandraHealthCheckDefaultQuerySpec extends CassandraSpec with CassandraLifecycle {

  "CassandraHealthCheck" must {
    "reply with successful health check result when plugin uses default query" in {
      val healthCheckResult = new CassandraHealthCheck(system)()
      healthCheckResult.futureValue shouldBe true
    }
  }
}

class CassandraHealthCheckCustomQueryNonEmptyResultSpec extends CassandraSpec(s"""
       akka.persistence.cassandra.healthcheck.health-check-cql="SELECT * FROM system.peers"
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

  "CassandraHealthCheck" must {
    "reply with successful health check result when plugin executes custom query and result is non-empty" in {
      val healthCheckResult = new CassandraHealthCheck(system)()
      healthCheckResult.futureValue shouldBe true
    }
  }
}

class CassandraHealthCheckCustomQueryEmptyResultSpec
    extends CassandraSpec(
      ConfigFactory.parseString(s"""
       akka.persistence.cassandra.healthcheck.health-check-cql="SELECT * FROM ignasi20210419002.always_empty"
    """), // FIXME the keyspace in the query must be the same as journalName below
      journalName = "ignasi20210419002", // FIXME getCallerName(getClass),
      snapshotName = "ignasi20210419002" // FIXME getCallerName(getClass),
    )
    with CassandraLifecycle
    with Eventually {

  override def beforeAll(): Unit = {
    super.beforeAll()

    // This test needs a table that's granted to be empty so we create a fake table for this unique purpose
    val tableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS ${journalName}.always_empty (
         |   persistence_id text,
         |   PRIMARY KEY (persistence_id))
         |""".stripMargin
    cluster.execute(tableCreation)
  }

  val eventualTimeout = Timeout(Span(10, Milliseconds))
  "CassandraHealthCheck" must {
    "reply with successful health check result when plugin executes custom query and result is empty" in {
      eventually(eventualTimeout) {
        println(" --------------------    checking health")
        val healthCheckResult = new CassandraHealthCheck(system)()
        healthCheckResult.futureValue shouldBe true
      }
    }
  }
}

class CassandraHealthCheckCustomFailingQuerySpec extends CassandraSpec(s"""
       akka.persistence.cassandra.healthcheck.health-check-cql="SELECT * FROM non_existing_keyspace.non_existing_table"
    """) with CassandraLifecycle {

  "CassandraHealthCheck" must {
    "reply with failed health check result when plugin executes custom query and it fails" in {
      val healthCheckResult = new CassandraHealthCheck(system)()
      healthCheckResult.futureValue shouldBe false
    }
  }
}
