package akka.persistence.cassandra.journal

import scala.collection.JavaConverters._
import com.datastax.driver.core._


trait CassandraConfigChecker extends CassandraStatements {
  def session: Session

  def initializePersistentConfig: Map[String, String] = {
    val result = session.execute(selectConfig).all().asScala
      .map(row => (row.getString("property"), row.getString("value"))).toMap

    result.get(CassandraJournalConfig.TargetPartitionProperty) match {
      case Some(oldValue) => assertCorrectPartitionSize(oldValue)
      case None =>
        val query = session.execute(writeConfig, CassandraJournalConfig.TargetPartitionProperty, config.targetPartitionSize.toString)
        if (!query.wasApplied()) {
          Option(query.one).map(_.getString("value")).foreach(assertCorrectPartitionSize)
        }
    }
    result + (CassandraJournalConfig.TargetPartitionProperty -> config.targetPartitionSize.toString)
  }

  private def assertCorrectPartitionSize(size: String) =
    require(size.toInt == config.targetPartitionSize, "Can't change target-partition-size")
}
