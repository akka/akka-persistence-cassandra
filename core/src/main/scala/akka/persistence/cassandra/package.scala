/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.UUID

import akka.Done
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, SerializedMeta }
import akka.serialization.Serialization

import scala.concurrent._
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._
import com.typesafe.config.{ Config, ConfigValueType }
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.serialization.AsyncSerializer
import akka.serialization.Serializers
import akka.annotation.InternalApi
import com.datastax.oss.driver.api.core.uuid.Uuids

package object cassandra {

  /** INTERNAL API */
  @InternalApi private[akka] val FutureDone: Future[Done] = Future.successful(Done)

  /** INTERNAL API */
  @InternalApi private[akka] val FutureUnit: Future[Unit] = Future.successful(())

  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS")

  /** INTERNAL API */
  @InternalApi private[akka] def formatOffset(uuid: UUID): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(Uuids.unixTimestamp(uuid)), ZoneOffset.UTC)
    s"$uuid (${timestampFormatter.format(time)})"
  }

  /** INTERNAL API */
  @InternalApi private[akka] def formatUnixTime(unixTime: Long): String = {
    val time =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTime), ZoneOffset.UTC)
    timestampFormatter.format(time)
  }

  /** INTERNAL API */
  @InternalApi private[akka] def serializeEvent(
      p: PersistentRepr,
      tags: Set[String],
      uuid: UUID,
      bucketSize: BucketSize,
      serialization: Serialization,
      system: ActorSystem)(implicit executionContext: ExecutionContext): Future[Serialized] =
    try {
      // use same clock source as the UUID for the timeBucket
      val timeBucket = TimeBucket(Uuids.unixTimestamp(uuid), bucketSize)

      def serializeMeta(): Option[SerializedMeta] =
        // meta data, if any
        p.metadata.map { m =>
          val m2 = m.asInstanceOf[AnyRef]
          val serializer = serialization.findSerializerFor(m2)
          val serManifest = Serializers.manifestFor(serializer, m2)
          val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
          SerializedMeta(metaBuf, serManifest, serializer.identifier)
        }

      val event: AnyRef = p.payload.asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(event)
      val serManifest = Serializers.manifestFor(serializer, event)

      serializer match {
        case asyncSer: AsyncSerializer =>
          Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSer.toBinaryAsync(event).map { bytes =>
              val serEvent = ByteBuffer.wrap(bytes)
              Serialized(
                p.persistenceId,
                p.sequenceNr,
                serEvent,
                tags,
                eventAdapterManifest = p.manifest,
                serManifest = serManifest,
                serId = serializer.identifier,
                p.writerUuid,
                serializeMeta(),
                uuid,
                timeBucket)
            }
          }

        case _ =>
          Future {
            // Serialization.serialize adds transport info
            val serEvent = ByteBuffer.wrap(serialization.serialize(event).get)
            Serialized(
              p.persistenceId,
              p.sequenceNr,
              serEvent,
              tags,
              eventAdapterManifest = p.manifest,
              serManifest = serManifest,
              serId = serializer.identifier,
              p.writerUuid,
              serializeMeta(),
              uuid,
              timeBucket)
          }
      }

    } catch {
      case NonFatal(e) => Future.failed(e)
    }

  /** INTERNAL API */
  @InternalApi private[akka] def indent(stmt: String, prefix: String): String =
    stmt.split('\n').mkString("\n" + prefix)

  /** INTERNAL API */
  @InternalApi private[akka] def getListFromConfig(config: Config, key: String): List[String] = {
    config.getValue(key).valueType() match {
      case ConfigValueType.LIST => config.getStringList(key).asScala.toList
      // case ConfigValueType.OBJECT is needed to handle dot notation (x.0=y x.1=z) due to Typesafe Config implementation quirk.
      // https://github.com/lightbend/config/blob/master/config/src/main/java/com/typesafe/config/impl/DefaultTransformer.java#L83
      case ConfigValueType.OBJECT => config.getStringList(key).asScala.toList
      case ConfigValueType.STRING => config.getString(key).split(",").toList
      case _                      => throw new IllegalArgumentException(s"$key should be a List, Object or String")
    }
  }

}
