package akka.persistence.cassandra

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

object JournalDseSpec {
  val config = ConfigFactory.parseString(s"""
   akka.persistence.cassandra.journal.keyspace=JournalDseSpec
   akka.persistence.cassandra.snapshot.keyspace=JournalDseSpec
                                 
   //# override-session-provider
   akka.persistence.cassandra {
     session-provider = "your.pack.DseSessionProvider"
   }
   //# override-session-provider
    """).withFallback(CassandraLifecycle.config)

}

class JournalDseSpec extends JournalSpec(JournalDseSpec.config) with CassandraLifecycle {
  override def systemName: String = "JournalDseSpec"
  override def supportsRejectingNonSerializableObjects = false
}
