/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import com.typesafe.config.Config

import scala.concurrent.duration._

@InternalApi private[akka] sealed trait BucketSize {
  val durationMillis: Long
}

private[akka] case object Day extends BucketSize {
  override val durationMillis: Long = 1.day.toMillis
}
private[akka] case object Hour extends BucketSize {
  override val durationMillis: Long = 1.hour.toMillis
}
private[akka] case object Minute extends BucketSize {
  override val durationMillis: Long = 1.minute.toMillis
}

private[akka] object BucketSize {
  def fromString(value: String): BucketSize = {
    Vector(Day, Hour, Minute).find(_.toString.toLowerCase == value.toLowerCase)
      .getOrElse(throw new IllegalArgumentException("Invalid value for bucket size: " + value))
  }
}

case class TableSettings(name: String, compactionStrategy: CassandraCompactionStrategy, gcGraceSeconds: Long, ttl: Option[Duration])

class CassandraJournalConfig(system: ActorSystem, config: Config) extends CassandraPluginConfig(system, config) with NoSerializationVerificationNeeded {
  val targetPartitionSize: Long = config.getLong(CassandraJournalConfig.TargetPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val replayMaxResultSize: Int = config.getInt("max-result-size-replay")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")

  // TODO this is now only used when deciding how to delete, remove this config and just
  // query what version of cassandra we're connected to and do the right thing
  val cassandra2xCompat: Boolean = config.getBoolean("cassandra-2x-compat")

  val maxConcurrentDeletes = config.getInt("max-concurrent-deletes")

  val queryPlugin = config.getString("query-plugin")

  val eventsByTagEnabled = config.getBoolean("events-by-tag.enabled")

  val bucketSize: BucketSize = BucketSize.fromString(config.getString("events-by-tag.bucket-size"))
  val tagTable = TableSettings(
    config.getString("events-by-tag.table"),
    CassandraCompactionStrategy(config.getConfig("events-by-tag.compaction-strategy")),
    config.getLong("events-by-tag.gc-grace-seconds"),
    if (config.hasPath("events-by-tag.time-to-live")) Some(config.getDuration("events-by-tag.time-to-live", TimeUnit.MILLISECONDS).millis) else None
  )

  val tagWriterSettings = TagWriterSettings(
    config.getInt("events-by-tag.max-message-batch-size"),
    config.getDuration("events-by-tag.flush-interval", TimeUnit.MILLISECONDS).millis,
    config.getDuration("events-by-tag.scanning-flush-interval", TimeUnit.MILLISECONDS).millis,
    config.getBoolean("pubsub-notification")
  )
}

object CassandraJournalConfig {
  val TargetPartitionProperty: String = "target-partition-size"
}

