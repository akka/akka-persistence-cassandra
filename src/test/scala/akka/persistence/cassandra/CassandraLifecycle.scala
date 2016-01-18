package akka.persistence.cassandra

import scala.concurrent.duration._
import org.scalatest._
import java.io.File
import akka.persistence.cassandra.testkit.CassandraLauncher

trait CassandraLifecycle extends BeforeAndAfterAll { this: Suite =>

  def systemName: String

  def cassandraConfigResource: String = CassandraLauncher.DefaultTestConfigResource

  override protected def beforeAll(): Unit = {
    val cassandraDirectory = new File("target/" + systemName)
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = cassandraConfigResource,
      clean = true,
      port = 0)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }
}
