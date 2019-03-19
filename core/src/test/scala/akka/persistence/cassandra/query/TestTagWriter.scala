/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.time.{ LocalDateTime, ZoneOffset }
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.formatOffset
import akka.persistence.cassandra.journal._
import akka.serialization.Serialization
import akka.serialization.Serializers
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs

private[akka] trait TestTagWriter {
  def system: ActorSystem
  val session: Session
  val serialization: Serialization
  val writePluginConfig: CassandraJournalConfig

  lazy val (preparedWriteTagMessage, preparedWriteTagMessageWithMeta) = {
    val writeStatements: CassandraStatements = new CassandraStatements {
      def config: CassandraJournalConfig = writePluginConfig
    }
    (session.prepare(writeStatements.writeTags(false)), session.prepare(writeStatements.writeTags(true)))
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
    val nowUuid = UUIDs.timeBased()
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

    val timeBucket = TimeBucket(UUIDs.unixTimestamp(uuid), bucketSize)

    val bs = preparedWriteTagMessage.bind()

    tags.foreach(tag => {
      bs.setString("tag_name", tag)
      bs.setLong("timebucket", timeBucket.key)
      bs.setUUID("timestamp", uuid)
      bs.setLong("tag_pid_sequence_nr", tagPidSequenceNr)
      bs.setBytes("event", serialized)
      bs.setString("event_manifest", pr.manifest)
      bs.setString("persistence_id", pr.persistenceId)
      bs.setInt("ser_id", serializer.identifier)
      bs.setString("ser_manifest", serManifest)
      bs.setString("writer_uuid", "ManualWrite")
      bs.setLong("sequence_nr", pr.sequenceNr)
      session.execute(bs)
    })

    system.log.debug("Written event: {} Uuid: {} Timebucket: {}", pr.payload, formatOffset(uuid), timeBucket)
  }
}
