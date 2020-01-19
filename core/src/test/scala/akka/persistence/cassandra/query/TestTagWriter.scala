/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.time.{ LocalDateTime, ZoneOffset }
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.BucketSize
import akka.persistence.cassandra.EventsByTagSettings
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal._
import akka.serialization.Serialization
import akka.serialization.Serializers
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids

private[akka] trait TestTagWriter {
  def system: ActorSystem
  def cluster: CqlSession
  val serialization: Serialization
  val settings: PluginSettings
  final def journalSettings: JournalSettings = settings.journalSettings
  final def eventsByTagSettings: EventsByTagSettings = settings.eventsByTagSettings

  lazy val (preparedWriteTagMessage, preparedWriteTagMessageWithMeta) = {
    val writeStatements: CassandraJournalStatements = new CassandraJournalStatements {
      def settings: PluginSettings = TestTagWriter.this.settings
    }
    (cluster.prepare(writeStatements.writeTags(false)), cluster.prepare(writeStatements.writeTags(true)))
  }

  def clearAllEvents(): Unit = {
    cluster.execute(s"truncate ${journalSettings.keyspace}.${eventsByTagSettings.tagTable.name}")
  }

  def writeTaggedEvent(
      time: LocalDateTime,
      pr: PersistentRepr,
      tags: Set[String],
      tagPidSequenceNr: Long,
      bucketSize: BucketSize): Unit = {
    val timestamp = time.toInstant(ZoneOffset.UTC).toEpochMilli
    write(pr, tags, tagPidSequenceNr, uuid(timestamp), bucketSize)
  }

  def writeTaggedEvent(
      persistent: PersistentRepr,
      tags: Set[String],
      tagPidSequenceNr: Long,
      bucketSize: BucketSize): Unit = {
    val nowUuid = Uuids.timeBased()
    write(persistent, tags, tagPidSequenceNr, nowUuid, bucketSize)
  }

  def writeTaggedEvent(
      persistent: PersistentRepr,
      tags: Set[String],
      tagPidSequenceNr: Long,
      uuid: UUID,
      bucketSize: BucketSize): Unit =
    write(persistent, tags, tagPidSequenceNr, uuid, bucketSize)

  private def write(
      pr: PersistentRepr,
      tags: Set[String],
      tagPidSequenceNr: Long,
      uuid: UUID,
      bucketSize: BucketSize): Unit = {
    val event = pr.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)
    val serManifest = Serializers.manifestFor(serializer, pr)
    val timeBucket = TimeBucket(Uuids.unixTimestamp(uuid), bucketSize)

    tags.foreach(tag => {
      val bs = preparedWriteTagMessage
        .bind()
        .setString("tag_name", tag)
        .setLong("timebucket", timeBucket.key)
        .setUuid("timestamp", uuid)
        .setLong("tag_pid_sequence_nr", tagPidSequenceNr)
        .setByteBuffer("event", serialized)
        .setString("event_manifest", pr.manifest)
        .setString("persistence_id", pr.persistenceId)
        .setInt("ser_id", serializer.identifier)
        .setString("ser_manifest", serManifest)
        .setString("writer_uuid", "ManualWrite")
        .setLong("sequence_nr", pr.sequenceNr)
      cluster.execute(bs)
    })

    system.log.debug(
      "Written event: {} Uuid: {} Timebucket: {} TagPidSeqNr: {}",
      pr.payload,
      formatOffset(uuid),
      timeBucket,
      tagPidSequenceNr)
  }
}
