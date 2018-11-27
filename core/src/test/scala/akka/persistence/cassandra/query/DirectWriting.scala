/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal.{CassandraJournalConfig, CassandraStatements, Hour, TimeBucket}
import akka.serialization.SerializationExtension
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, Suite}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

import akka.serialization.Serializers

trait DirectWriting extends BeforeAndAfterAll {
  self: Suite =>

  def system: ActorSystem
  private lazy val serialization = SerializationExtension(system)
  private lazy val writePluginConfig =
    new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))

  private lazy val session = {
    Await.result(writePluginConfig.sessionProvider.connect()(system.dispatcher), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    Try {
      session.close()
      session.getCluster.close()
    }
    super.afterAll()
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

    val serManifest = Serializers.manifestFor(serializer, persistent)

    val bs = preparedWriteMessage.bind()
    bs.setString("persistence_id", persistent.persistenceId)
    bs.setLong("partition_nr", 1L)
    bs.setLong("sequence_nr", persistent.sequenceNr)
    val nowUuid = UUIDs.timeBased()
    val now = UUIDs.unixTimestamp(nowUuid)
    bs.setUUID("timestamp", nowUuid)
    bs.setString("timebucket", TimeBucket(now, Hour).key.toString)
    bs.setInt("ser_id", serializer.identifier)
    bs.setString("ser_manifest", serManifest)
    bs.setString("event_manifest", persistent.manifest)
    bs.setBytes("event", serialized)
    session.execute(bs)
    system.log.debug("Directly wrote payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

}
