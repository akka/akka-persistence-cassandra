/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.util.Locale

import scala.collection.immutable.HashMap
import scala.concurrent.duration.Duration

import com.typesafe.config.{ Config, ConfigValueType }

import akka.persistence.cassandra.CassandraPluginConfig
import akka.util.Helpers.{ ConfigOps, Requiring }

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val targetPartitionSize: Int = config.getInt(CassandraJournalConfig.TargetPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val replayMaxResultSize: Int = config.getInt("max-result-size-replay")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")
  val deleteRetries: Int = config.getInt("delete-retries")
  val writeRetries: Int = config.getInt("write-retries")
  val cassandra2xCompat: Boolean = config.getBoolean("cassandra-2x-compat")
  val enableEventsByTagQuery: Boolean = !cassandra2xCompat && config.getBoolean("enable-events-by-tag-query")
  val eventsByTagView: String = config.getString("events-by-tag-view")
  val pubsubMinimumInterval: Duration = {
    val key = "pubsub-minimum-interval"
    config.getString(key).toLowerCase(Locale.ROOT) match {
      case "off" ⇒ Duration.Undefined
      case _     ⇒ config.getMillisDuration(key) requiring (_ > Duration.Zero, key + " > 0s, or off")
    }
  }

  val maxTagsPerEvent: Int = 3
  val tags: HashMap[String, Int] = {
    import scala.collection.JavaConverters._
    config.getConfig("tags").entrySet.asScala.collect {
      case entry if entry.getValue.valueType == ConfigValueType.NUMBER =>
        val tag = entry.getKey
        val tagId = entry.getValue.unwrapped.asInstanceOf[Number].intValue
        require(
          1 <= tagId && tagId <= 3,
          s"Tag identifer for [$tag] must be a 1, 2, or 3, was [$tagId]. " +
            s"Max $maxTagsPerEvent tags per event is supported."
        )
        tag -> tagId
    }(collection.breakOut)
  }

  /**
   * Will be 0 if [[#enableEventsByTagQuery]] is disabled,
   * will be 1 if [[#tags]] is empty, otherwise the number of configured
   * distinct tag identifiers.
   */
  def maxTagId: Int = if (!enableEventsByTagQuery) 0 else if (tags.isEmpty) 1 else tags.values.max
}

object CassandraJournalConfig {
  val TargetPartitionProperty: String = "target-partition-size"
}
