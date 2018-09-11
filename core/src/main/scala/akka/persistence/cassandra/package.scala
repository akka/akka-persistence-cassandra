/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.Executor

import akka.persistence.cassandra.journal.{ BucketSize, TimeBucket }
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, SerializedMeta }
import akka.serialization.Serialization
import com.datastax.driver.core.utils.UUIDs
import com.google.common.util.concurrent.ListenableFuture
import scala.concurrent._
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.serialization.AsyncSerializer
import akka.serialization.Serializers
import akka.Done
import akka.annotation.InternalApi

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

  val FutureDone: Future[Done] = Future.successful(Done)

  def serializeEvent(p: PersistentRepr, tags: Set[String], uuid: UUID,
                     bucketSize: BucketSize, serialization: Serialization, system: ActorSystem)(implicit executionContext: ExecutionContext): Future[Serialized] = {
    try {
      // use same clock source as the UUID for the timeBucket
      val timeBucket = TimeBucket(UUIDs.unixTimestamp(uuid), bucketSize)

      def serializeMeta(): Option[SerializedMeta] = {
        // meta data, if any
        p.payload match {
          case EventWithMetaData(_, m) =>
            val m2 = m.asInstanceOf[AnyRef]
            val serializer = serialization.findSerializerFor(m2)
            val serManifest = Serializers.manifestFor(serializer, m2)
            val metaBuf = ByteBuffer.wrap(serialization.serialize(m2).get)
            Some(SerializedMeta(metaBuf, serManifest, serializer.identifier))
          case _ => None
        }
      }

      val event: AnyRef = (p.payload match {
        case EventWithMetaData(evt, _) => evt // unwrap
        case evt                       => evt
      }).asInstanceOf[AnyRef]

      val serializer = serialization.findSerializerFor(event)
      val serManifest = Serializers.manifestFor(serializer, event)

      serializer match {
        case asyncSer: AsyncSerializer =>
          Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSer.toBinaryAsync(event).map { bytes =>
              val serEvent = ByteBuffer.wrap(bytes)
              Serialized(p.persistenceId, p.sequenceNr, serEvent, tags,
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
            Serialized(p.persistenceId, p.sequenceNr, serEvent, tags,
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
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def indent(stmt: String, prefix: String): String = {
    stmt.split('\n').mkString("\n" + prefix)
  }

}
