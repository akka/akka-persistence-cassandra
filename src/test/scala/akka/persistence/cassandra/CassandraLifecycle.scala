package akka.persistence.cassandra

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

trait CassandraLifecycle extends BeforeAndAfterAll { this: Suite =>
  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    super.afterAll()
  }
}
