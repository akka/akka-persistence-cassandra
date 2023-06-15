/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import com.datastax.oss.driver.api.core.cql.Row
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.persistence.PersistentRepr
import akka.persistence.cassandra.journal._
import akka.persistence.cassandra.journal.CassandraJournal._
import akka.annotation.InternalApi
import java.{ util => ju }

import akka.util.OptionVal
import akka.serialization.Serialization
import akka.util.ccompat.JavaConverters._
import java.nio.ByteBuffer

import com.datastax.oss.protocol.internal.util.Bytes
import akka.actor.ActorSystem
import akka.persistence.query.TimeBasedUUID

/**
 * An Extractor takes reads a row from the messages table. There are different extractors
 * for creating PersistentRepr, TaggedPersistentRepr, and RawEvent.
 *
 * INTERNAL API
 */
@InternalApi private[akka] object Extractors {

  /**
   * A [[PersistentRepr]] along with its tags and offset.
   */
  case class TaggedPersistentRepr(pr: PersistentRepr, tags: Set[String], offset: ju.UUID) {
    def sequenceNr: Long = pr.sequenceNr
  }

  case class OptionalTagged(sequenceNr: Long, tagged: OptionVal[TaggedPersistentRepr])

  /**
   * A RawEvent is an event from the messages table in its serialized form
   * Used for reading and writing between the messages table and the tag_views
   * table without serializing/de-serializing
   */
  case class RawEvent(sequenceNr: Long, serialized: Serialized)

  abstract class Extractor[T] {
    def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[T]
  }

  final case class SeqNrValue(sequenceNr: Long)

  private[akka] def deserializeRawEvent(
      system: ActorSystem,
      bucketSize: BucketSize,
      columnDefinitionCache: ColumnDefinitionCache,
      tags: Set[String],
      serialization: Serialization,
      row: Row): Future[RawEvent] = {
    import system._
    val timeUuid = row.getUuid("timestamp")
    val sequenceNr = row.getLong("sequence_nr")
    val meta = if (columnDefinitionCache.hasMetaColumns(row)) {
      val m = row.getByteBuffer("meta")
      Option(m).map(SerializedMeta(_, row.getString("meta_ser_manifest"), row.getInt("meta_ser_id")))
    } else {
      None
    }

    def deserializeEvent(): Future[RawEvent] = {
      Future.successful(
        RawEvent(
          sequenceNr,
          Serialized(
            row.getString("persistence_id"),
            row.getLong("sequence_nr"),
            row.getByteBuffer("event"),
            tags,
            row.getString("event_manifest"),
            row.getString("ser_manifest"),
            row.getInt("ser_id"),
            row.getString("writer_uuid"),
            meta,
            timeUuid,
            timeBucket = TimeBucket(timeUuid, bucketSize))))
    }

    if (columnDefinitionCache.hasMessageColumn(row)) {
      row.getByteBuffer("message") match {
        case null  => deserializeEvent()
        case bytes =>
          // This is an event from version 0.6 and earlier that used to serialise the PersistentRepr in the
          // message column rather than the event column
          val pr = serialization.deserialize(Bytes.getArray(bytes), classOf[PersistentRepr]).get
          serializeEvent(pr, tags, timeUuid, bucketSize, serialization, system).map { serEvent =>
            RawEvent(sequenceNr, serEvent)
          }
      }
    } else {
      deserializeEvent()
    }
  }

  /**
   * Extractor that does not de-serialize the event.
   * It does not support versions older than 0.60 that serialized
   * the PersistentRepr into the message column
   *
   * @param bucketSize for calculating which bucket each event should
   */
  def rawEvent(bucketSize: BucketSize, serialization: Serialization, system: ActorSystem): Extractor[RawEvent] = {
    new Extractor[RawEvent] {

      // Could  make this an extension? Too global?
      val columnDefinitionCache = new ColumnDefinitionCache

      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[RawEvent] = {
        val tags = extractTags(row, columnDefinitionCache)
        deserializeRawEvent(system, bucketSize, columnDefinitionCache, tags, serialization, row)
      }
    }
  }

  def persistentRepr(e: EventDeserializer, s: Serialization): Extractor[PersistentRepr] =
    new Extractor[PersistentRepr] {
      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[PersistentRepr] =
        extractPersistentRepr(row, e, s, async)
    }

  def persistentReprAndOffset(e: EventDeserializer, s: Serialization): Extractor[(PersistentRepr, TimeBasedUUID)] =
    new Extractor[(PersistentRepr, TimeBasedUUID)] {
      override def extract(row: Row, async: Boolean)(
          implicit ec: ExecutionContext): Future[(PersistentRepr, TimeBasedUUID)] =
        extractPersistentRepr(row, e, s, async).map(repr => repr -> TimeBasedUUID(row.getUuid("timestamp")))
    }

  def taggedPersistentRepr(ed: EventDeserializer, s: Serialization): Extractor[TaggedPersistentRepr] =
    new Extractor[TaggedPersistentRepr] {
      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[TaggedPersistentRepr] =
        extractPersistentRepr(row, ed, s, async).map { persistentRepr =>
          val tags = extractTags(row, ed.columnDefinitionCache)
          TaggedPersistentRepr(persistentRepr, tags, row.getUuid("timestamp"))
        }
    }

  def optionalTaggedPersistentRepr(ed: EventDeserializer, s: Serialization): Extractor[OptionalTagged] =
    new Extractor[OptionalTagged] {
      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[OptionalTagged] = {
        val seqNr = row.getLong("sequence_nr")
        val tags = extractTags(row, ed.columnDefinitionCache)
        if (tags.isEmpty) {
          // no tags, no need to extract more
          Future.successful(OptionalTagged(seqNr, OptionVal.None))
        } else {
          extractPersistentRepr(row, ed, s, async).map { persistentRepr =>
            val tagged = TaggedPersistentRepr(persistentRepr, tags, row.getUuid("timestamp"))
            OptionalTagged(seqNr, OptionVal.Some(tagged))
          }
        }
      }
    }

  // TODO performance improvement could be to use another query that is not "select *"
  def sequenceNumber(ed: EventDeserializer, s: Serialization): Extractor[SeqNrValue] =
    new Extractor[SeqNrValue] {
      override def extract(row: Row, async: Boolean)(implicit ec: ExecutionContext): Future[SeqNrValue] =
        Future.successful(SeqNrValue(row.getLong("sequence_nr")))
    }

  private def extractPersistentRepr(row: Row, ed: EventDeserializer, s: Serialization, async: Boolean)(
      implicit ec: ExecutionContext): Future[PersistentRepr] = {

    def deserializeEvent(): Future[PersistentRepr] = {
      ed.deserializeEvent(row, async).map {
        case DeserializedEvent(payload, metadata) =>
          val repr = PersistentRepr(
            payload,
            sequenceNr = row.getLong("sequence_nr"),
            persistenceId = row.getString("persistence_id"),
            manifest = row.getString("event_manifest"), // manifest for event adapters
            deleted = false,
            sender = null,
            writerUuid = row.getString("writer_uuid"))
          metadata match {
            case OptionVal.Some(m) => repr.withMetadata(m)
            case _                 => repr
          }
      }
    }

    if (ed.columnDefinitionCache.hasMessageColumn(row)) {
      row.getByteBuffer("message") match {
        case null  => deserializeEvent()
        case bytes =>
          // For backwards compatibility, reading serialized PersistentRepr from "message" column.
          // Used in v0.6 and earlier. In later versions the "event" column is used for the serialized event.
          Future.successful(persistentFromByteBuffer(s, bytes))
      }
    } else {
      deserializeEvent()
    }
  }

  /**
   * Extract tags from either tag1, tag2, tag3 or the current tags column
   */
  private def extractTags(row: Row, columnDefinitionCache: ColumnDefinitionCache): Set[String] = {
    // TODO can be removed in 1.0, this is only used during migration from the old version on initial recovery
    // Unless we allow migration from pre 0.80 versions to 1.0?
    val oldTags: Set[String] =
      if (columnDefinitionCache.hasOldTagsColumns(row)) {
        (1 to 3).foldLeft(Set.empty[String]) {
          case (acc, i) =>
            val tag = row.getString(s"tag$i")
            if (tag != null) acc + tag
            else acc
        }
      } else Set.empty

    val newTags: Set[String] =
      if (columnDefinitionCache.hasTagsColumn(row))
        row.getSet("tags", classOf[String]).asScala.toSet
      else Set.empty

    oldTags.union(newTags)
  }

  def persistentFromByteBuffer(serialization: Serialization, b: ByteBuffer): PersistentRepr =
    // we know that such old rows can't have meta data because that feature was added later
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
}
