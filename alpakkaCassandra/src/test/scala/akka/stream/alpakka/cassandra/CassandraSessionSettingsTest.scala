package akka.stream.alpakka.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpec }

class CassandraSessionSettingsTest extends WordSpec with Matchers {

  "settings" should {
    "read" in {
      val config = ConfigFactory.load()
      val s = CassandraSessionSettings(config.getConfig(CassandraSessionSettings.ConfigPath))
      s.fetchSize shouldBe 250
      s.connectionRetries shouldBe 3
      s.readConsistency shouldBe ConsistencyLevel.QUORUM
      s.writeConsistency shouldBe ConsistencyLevel.QUORUM
    }
  }

}
