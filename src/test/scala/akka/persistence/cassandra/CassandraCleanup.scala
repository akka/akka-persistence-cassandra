package akka.persistence.cassandra

import akka.testkit.TestKitBase

import com.datastax.driver.core.Cluster

import org.scalatest._

trait CassandraCleanup extends BeforeAndAfterAll { this: TestKitBase with Suite =>
  val journalConfig = system.settings.config.getConfig("cassandra-journal")
  val snapshotConfig = system.settings.config.getConfig("cassandra-snapshot-store")

  override def afterAll(): Unit = {
    super.afterAll()
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE ${journalConfig.getString("keyspace")}")
    session.execute(s"DROP KEYSPACE ${snapshotConfig.getString("keyspace")}")
    session.close()
    cluster.close()
  }
}
