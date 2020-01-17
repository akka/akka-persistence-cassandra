/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.journal.Hour
import akka.persistence.cassandra.journal.TimeBucket
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait DirectWriting extends BeforeAndAfterAll {
  self: Suite =>

  def system: ActorSystem
  private lazy val serialization = SerializationExtension(system)
  private lazy val writePluginConfig =
    new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-plugin"))

  def cluster: CqlSession

  private lazy val writeStatements: CassandraStatements = new CassandraStatements {
    def config: CassandraJournalConfig = writePluginConfig
  }

  private lazy val preparedWriteMessage = cluster.prepare(writeStatements.writeMessage(withMeta = false))

  private lazy val preparedDeleteMessage = cluster.prepare(writeStatements.deleteMessage)

  protected def writeTestEvent(persistent: PersistentRepr, partitionNr: Long = 1L): Unit = {
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)
    val nowUuid = Uuids.timeBased()
    val now = Uuids.unixTimestamp(nowUuid)
    val serManifest = Serializers.manifestFor(serializer, persistent)

    val bs = preparedWriteMessage
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
