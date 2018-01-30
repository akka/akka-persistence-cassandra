/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.Executor

import akka.persistence.cassandra.journal.{ BucketSize, TimeBucket }
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, SerializedMeta }
import akka.serialization.{ Serialization, SerializerWithStringManifest }
import com.datastax.driver.core.utils.UUIDs
import com.google.common.util.concurrent.ListenableFuture

import scala.concurrent._
import scala.language.implicitConversions
import scala.util.Try

package object cassandra {
  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS")

  // TODO we should use the more explicit ListenableFutureConverter asScala instead
  implicit def listenableFutureToFuture[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    val promise = Promise[A]
    lf.addListener(new Runnable {
      def run() = promise.complete(Try(lf.get()))
    }, executionContext.asInstanceOf[Executor])
    promise.future
  }

  implicit class ListenableFutureConverter[A](val lf: ListenableFuture[A]) extends AnyVal {
    def asScala(implicit ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]
      lf.addListener(new Runnable {
        def run() = promise.complete(Try(lf.get()))
      }, ec.asInstanceOf[Executor])
      promise.future
    }
  }

  def formatOffset(uuid: UUID): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(UUIDs.unixTimestamp(uuid)), ZoneOffset.UTC)
    s"$uuid (${timestampFormatter.format(time)})"
  }

  def serializeEvent(p: PersistentRepr, tags: Set[String], uuid: UUID,
                     bucketSize: BucketSize, serialization: Serialization,
                     transportInformation: Option[Serialization.Information]): Serialized = {
    // use same clock source as the UUID for the timeBucket
    val timeBucket = TimeBucket(UUIDs.unixTimestamp(uuid), bucketSize)

    def serializeMeta(): Option[SerializedMeta] = {
      // meta data, if any
      p.payload match {
        case EventWithMetaData(_, m) =>
          val m2 = m.asInstanceOf[AnyRef]
          val serializer = serialization.findSerializerFor(m2)
          val serManifest = serializer match {
            case ser2: SerializerWithStringManifest ⇒
              ser2.manifest(m2)
            case _ ⇒
              if (serializer.includeManifest) m2.getClass.getName
              else PersistentRepr.Undefined
          }
          val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
          Some(SerializedMeta(metaBuf, serManifest, serializer.identifier))
        case _ => None
      }
    }

    def doSerializeEvent(): Serialized = {
      val event: AnyRef = (p.payload match {
        case EventWithMetaData(evt, _) => evt // unwrap
        case evt                       => evt
      }).asInstanceOf[AnyRef]

      val serializer = serialization.findSerializerFor(event)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest ⇒
          ser2.manifest(event)
        case _ ⇒
          if (serializer.includeManifest) event.getClass.getName
          else PersistentRepr.Undefined
      }
      val serEvent = ByteBuffer.wrap(serialization.serialize(event).get)

      Serialized(p.persistenceId, p.sequenceNr, serEvent, tags, p.manifest, serManifest,
        serializer.identifier, p.writerUuid, serializeMeta(), uuid, timeBucket)
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) ⇒ Serialization.currentTransportInformation.withValue(ti) {
        doSerializeEvent()
      }
      case None ⇒ doSerializeEvent()
    }
  }
}
