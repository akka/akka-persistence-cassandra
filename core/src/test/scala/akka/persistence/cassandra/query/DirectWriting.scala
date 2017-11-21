/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, CassandraStatements, TimeBucket }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import com.datastax.driver.core.utils.UUIDs

import scala.concurrent.Await
import scala.concurrent.duration._

trait DirectWriting {

  def system: ActorSystem
  private lazy val serialization = SerializationExtension(system)
  private lazy val writePluginConfig = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))

  private lazy val session = {

    Await.result(writePluginConfig.sessionProvider.connect()(system.dispatcher), 5.seconds)
  }

  private lazy val preparedWriteMessage = {
    val writeStatements: CassandraStatements = new CassandraStatements {
      def config: CassandraJournalConfig = writePluginConfig
    }
    session.prepare(writeStatements.writeMessage(withMeta = false))
  }

  protected def writeTestEvent(persistent: PersistentRepr): Unit = {
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)

    val serManifest = serializer match {
      case ser2: SerializerWithStringManifest ⇒
        ser2.manifest(persistent)
      case _ ⇒
        if (serializer.includeManifest) persistent.getClass.getName
        else PersistentRepr.Undefined
    }

    val bs = preparedWriteMessage.bind()
    bs.setString("persistence_id", persistent.persistenceId)
    bs.setLong("partition_nr", 1L)
    bs.setLong("sequence_nr", persistent.sequenceNr)
    val nowUuid = UUIDs.timeBased()
    val now = UUIDs.unixTimestamp(nowUuid)
    bs.setUUID("timestamp", nowUuid)
    bs.setString("timebucket", TimeBucket(now).key)
    bs.setInt("ser_id", serializer.identifier)
    bs.setString("ser_manifest", serManifest)
    bs.setString("event_manifest", persistent.manifest)
    bs.setBytes("event", serialized)
    session.execute(bs)
    system.log.debug("Directly wrote payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

}
