package akka.persistence.cassandra.journal

import com.typesafe.config.Config
import akka.persistence.cassandra.CassandraPluginConfig
import com.typesafe.config.ConfigValueType
import scala.collection.immutable.HashMap

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val targetPartitionSize: Int = config.getInt(CassandraJournalConfig.TargetPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val replayMaxResultSize: Int = config.getInt("max-result-size-replay")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")
  val deleteRetries: Int = config.getInt("delete-retries")
  val writeRetries: Int = config.getInt("write-retries")
  val eventsByTagView: String = config.getString("events-by-tag-view")

  val maxTagsPerEvent: Int = 3
  val tags: HashMap[String, Int] = {
    import scala.collection.JavaConverters._
    config.getConfig("tags").entrySet.asScala.collect {
      case entry if entry.getValue.valueType == ConfigValueType.NUMBER =>
        val tag = entry.getKey
        val tagId = entry.getValue.unwrapped.asInstanceOf[Number].intValue
        require(1 <= tagId && tagId <= 3,
          s"Tag identifer for [$tag] must be a 1, 2, or 3, was [$tagId]. " +
            s"Max $maxTagsPerEvent tags per event is supported.")
        tag -> tagId
    }(collection.breakOut)
  }

  def maxTagId: Int = if (tags.isEmpty) 1 else tags.values.max
}

object CassandraJournalConfig {
  val TargetPartitionProperty: String = "target-partition-size"
}
