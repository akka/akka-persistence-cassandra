package akka.persistence.cassandra

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

object JournalDseSpec {
  val config = ConfigFactory.parseString(s"""
   cassandra-plugin.journal.keyspace=JournalDseSpec
   cassandra-plugin.snapshot.keyspace=JournalDseSpec
                                 
   //# override-session-provider
   cassandra-plugin {
     session-provider = "your.pack.DseSessionProvider"
   }
   //# override-session-provider
    """).withFallback(CassandraLifecycle.config)

}

class JournalDseSpec extends JournalSpec(JournalDseSpec.config) with CassandraLifecycle {
  override def systemName: String = "JournalDseSpec"
  override def supportsRejectingNonSerializableObjects = false
}
