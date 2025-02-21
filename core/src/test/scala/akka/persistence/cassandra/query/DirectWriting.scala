/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.persistence.{ PersistentRepr, SnapshotMetadata }
import akka.persistence.cassandra.Hour
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraJournalStatements
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.snapshot.{ CassandraSnapshotStatements, CassandraSnapshotStore }
import akka.persistence.cassandra.snapshot.CassandraSnapshotStore.SnapshotSerialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.concurrent.{ ExecutionContext, Future }

trait DirectWriting extends BeforeAndAfterAll {
  self: Suite =>

  def system: ActorSystem
  private lazy val serialization = SerializationExtension(system)
  private lazy val settings = PluginSettings(system)
  private lazy implicit val ec: ExecutionContext = system.dispatcher

  def cluster: CqlSession

  private lazy val writeStatements: CassandraJournalStatements = new CassandraJournalStatements(settings)
  private lazy val snapshotStatements: CassandraSnapshotStatements = new CassandraSnapshotStatements(
    settings.snapshotSettings)
  private lazy val snapshotSerialization = new SnapshotSerialization(system)(system.dispatcher)

  private lazy val preparedWriteMessage = cluster.prepare(writeStatements.writeMessage(withMeta = true))
  private lazy val preparedDeleteMessage = cluster.prepare(writeStatements.deleteMessage)
  private lazy val preparedWriteSnapshot = cluster.prepare(snapshotStatements.writeSnapshot(withMeta = false))

  def writeTestSnapshot(snapshotMeta: SnapshotMetadata, snapshot: AnyRef): Future[Unit] = {
    snapshotSerialization.serialize(snapshot, None).map { ser =>
      val bound = CassandraSnapshotStore.prepareSnapshotWrite(preparedWriteSnapshot, snapshotMeta, ser)
      cluster.execute(bound)
      ()
    }
  }

  def writeTestEvent(persistent: PersistentRepr, partitionNr: Long = 1L): Unit = {
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)
    val nowUuid = Uuids.timeBased()
    val now = Uuids.unixTimestamp(nowUuid)
    val serManifest = Serializers.manifestFor(serializer, persistent)

    var bs = preparedWriteMessage
      .bind()
      .setString("persistence_id", persistent.persistenceId)
      .setLong("partition_nr", partitionNr)
      .setLong("sequence_nr", persistent.sequenceNr)
      .setUuid("timestamp", nowUuid)
      .setString("timebucket", TimeBucket(now, Hour).key.toString)
      .setInt("ser_id", serializer.identifier)
      .setString("ser_manifest", serManifest)
      .setString("event_manifest", persistent.manifest)
      .setByteBuffer("event", serialized)

    bs = persistent.metadata match {
      case Some(meta) =>
        val metaPayload = meta.asInstanceOf[AnyRef]
        val metaSerializer = serialization.findSerializerFor(metaPayload)
        val metaSerialized = ByteBuffer.wrap(serialization.serialize(metaPayload).get)
        val metaSerializedManifest = Serializers.manifestFor(metaSerializer, metaPayload)
        bs.setString("meta_ser_manifest", metaSerializedManifest)
          .setInt("meta_ser_id", metaSerializer.identifier)
          .setByteBuffer("meta", metaSerialized)
      case _ =>
        bs
    }

    cluster.execute(bs)
    system.log.debug("Directly wrote payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

  protected def deleteTestEvent(persistent: PersistentRepr, partitionNr: Long = 1L): Unit = {

    val bs = preparedDeleteMessage
      .bind()
      .setString("persistence_id", persistent.persistenceId)
      .setLong("partition_nr", partitionNr)
      .setLong("sequence_nr", persistent.sequenceNr)
    cluster.execute(bs)
    system.log.debug("Directly deleted payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

}
