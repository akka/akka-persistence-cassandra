/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.collection.immutable
import java.util.concurrent.TimeUnit

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.compaction.CassandraCompactionStrategy
import akka.persistence.cassandra.journal.TagWriter.TagWriterSettings
import com.typesafe.config.Config
import scala.concurrent.duration._

import akka.persistence.cassandra.CassandraPluginConfig.getReplicationStrategy
import akka.persistence.cassandra.getListFromConfig

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

// Not to be used for real production apps. Just to make testing bucket transitions easier.
private[akka] case object Second extends BucketSize {
  override val durationMillis: Long = 1.second.toMillis
}

private[akka] object BucketSize {
  def fromString(value: String): BucketSize =
    Vector(Day, Hour, Minute, Second)
      .find(_.toString.toLowerCase == value.toLowerCase)
      .getOrElse(throw new IllegalArgumentException("Invalid value for bucket size: " + value))
}

case class TableSettings(
    name: String,
    compactionStrategy: CassandraCompactionStrategy,
    gcGraceSeconds: Long,
    ttl: Option[Duration])

class CassandraJournalConfig(system: ActorSystem, config: Config)
    extends CassandraPluginConfig(system, config)
    with NoSerializationVerificationNeeded {

  private val writeConfig = config.getConfig("write")
  private val eventsByTagConfig = config.getConfig("events-by-tag")

  val writeProfile: String = config.getString("write-profile")
  val readProfile: String = config.getString("read-profile")

  CassandraPluginConfig.checkProfile(system, writeProfile)
  CassandraPluginConfig.checkProfile(system, readProfile)

  val table: String = writeConfig.getString("table")
  val metadataTable: String = writeConfig.getString("metadata-table")

  val tableCompactionStrategy: CassandraCompactionStrategy =
    CassandraCompactionStrategy(writeConfig.getConfig("table-compaction-strategy"))

  val replicationStrategy: String = getReplicationStrategy(
    writeConfig.getString("replication-strategy"),
    writeConfig.getInt("replication-factor"),
    getListFromConfig(writeConfig, "data-center-replication-factors"))

  val gcGraceSeconds: Long = writeConfig.getLong("gc-grace-seconds")

  val targetPartitionSize: Long = writeConfig.getLong("target-partition-size")
  val maxMessageBatchSize: Int = writeConfig.getInt("max-message-batch-size")

  // TODO this is now only used when deciding how to delete, remove this config and just
  // query what version of cassandra we're connected to and do the right thing
  val cassandra2xCompat: Boolean = config.getBoolean("cassandra-2x-compat")

  val maxConcurrentDeletes: Int = writeConfig.getInt("max-concurrent-deletes")

  val supportDeletes: Boolean = writeConfig.getBoolean("support-deletes")

  val eventsByTagEnabled: Boolean = eventsByTagConfig.getBoolean("enabled")

  val bucketSize: BucketSize =
    BucketSize.fromString(eventsByTagConfig.getString("bucket-size"))

  if (bucketSize == Second) {
    system.log.warning("Do not use Second bucket size in production. It is meant for testing purposes only.")
  }

  val tagTable = TableSettings(
    eventsByTagConfig.getString("table"),
    CassandraCompactionStrategy(eventsByTagConfig.getConfig("compaction-strategy")),
    eventsByTagConfig.getLong("gc-grace-seconds"),
    if (eventsByTagConfig.hasPath("time-to-live"))
      Some(eventsByTagConfig.getDuration("time-to-live", TimeUnit.MILLISECONDS).millis)
    else None)

  private val pubsubNotificationInterval: Duration = config.getString("pubsub-notification").toLowerCase match {
    case "on" | "true"   => 100.millis
    case "off" | "false" => Duration.Undefined
    case _               => config.getDuration("pubsub-notification", TimeUnit.MILLISECONDS).millis
  }

  val tagWriterSettings = TagWriterSettings(
    eventsByTagConfig.getInt("max-message-batch-size"),
    eventsByTagConfig.getDuration("flush-interval", TimeUnit.MILLISECONDS).millis,
    eventsByTagConfig.getDuration("scanning-flush-interval", TimeUnit.MILLISECONDS).millis,
    pubsubNotificationInterval)

  val coordinatedShutdownOnError: Boolean = config.getBoolean("coordinated-shutdown-on-error")

  /**
   * The Cassandra Statement that can be used to create the configured keyspace.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraJournalConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-journal")).createKeyspaceStatement
   * }}}
   *
   * @see [[CassandraJournalConfig#createTablesStatements]]
   */
  def createKeyspaceStatement: String =
    statements.createKeyspace

  /**
   * Scala API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraJournalConfig(actorSystem, actorSystem.settings.config.getConfig("cassandra-journal")).createTablesStatements
   * }}}
   * *
   * * @see [[CassandraJournalConfig#createKeyspaceStatement]]
   */
  def createTablesStatements: immutable.Seq[String] =
    statements.createTable ::
    statements.createTagsTable ::
    statements.createTagsProgressTable ::
    statements.createTagScanningTable ::
    statements.createMetadataTable ::
    Nil

  /**
   * Java API: The Cassandra statements that can be used to create the configured tables.
   *
   * This can be queried in for example a startup script without accessing the actual
   * Cassandra plugin actor.
   *
   * {{{
   * new CassandraJournalConfig(actorSystem, actorSystem.settings().config().getConfig("cassandra-journal")).getCreateTablesStatements();
   * }}}
   * *
   * * @see [[CassandraJournalConfig#createKeyspaceStatement]]
   */
  def getCreateTablesStatements: java.util.List[String] = {
    import scala.collection.JavaConverters._
    createTablesStatements.asJava
  }

  private def statements: CassandraStatements =
    new CassandraStatements {
      override def config: CassandraJournalConfig = CassandraJournalConfig.this
    }
}
