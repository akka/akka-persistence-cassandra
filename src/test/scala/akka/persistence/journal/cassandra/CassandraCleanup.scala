package akka.persistence.journal.cassandra

import java.io.File

import akka.testkit.TestKitBase

import com.datastax.driver.core.Cluster

import org.apache.commons.io.FileUtils
import org.scalatest._

trait CassandraCleanup extends BeforeAndAfterAll { this: TestKitBase with Suite =>
  val journalConfig = system.settings.config.getConfig("cassandra-journal")
  val snapshotConfig = system.settings.config.getConfig("akka.persistence.snapshot-store.local")

  override def afterAll(): Unit = {
    super.afterAll()
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE ${journalConfig.getString("keyspace")}")
    FileUtils.deleteDirectory(new File(snapshotConfig.getString("dir")))
  }
}
