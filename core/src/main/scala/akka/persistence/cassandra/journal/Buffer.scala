/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.annotation.InternalApi
import akka.persistence.cassandra.journal.TagWriter.{ timeUuidOrdering, AwaitingWrite }
import akka.util.{ OptionVal, UUIDComparator }

/**
 * INTERNAL API
 *
 * Buffered events waiting to be written.
 * The next batch is maintained in `nextBatch` and will never contain more than the `batchSize`
 * or events from different time buckets.
 *
 * Events should be added and then call shouldWrite() to see if a batch is ready to be written.
 * Once a write is complete call `writeComplete` to discard the events in `nextBatch` and take
 * events from `pending` for the next batch.
 */
@InternalApi
private[akka] case class Buffer(
    batchSize: Int,
    size: Int,
    nextBatch: Vector[AwaitingWrite],
    pending: Vector[AwaitingWrite],
    writeRequired: Boolean) {
  require(batchSize > 0)

  def isEmpty: Boolean = nextBatch.isEmpty

  def nonEmpty: Boolean = nextBatch.nonEmpty

  def remove(pid: String): Buffer = {
    val (toFilter, without) = nextBatch.partition(_.events.head._1.persistenceId == pid)
    val filteredPending = pending.filterNot(_.events.head._1.persistenceId == pid)
    val removed = toFilter.foldLeft(0)((acc, next) => acc + next.events.size)
    copy(size = size - removed, nextBatch = without, pending = filteredPending)
  }

  /**
   * Any time a new time bucket is received or the max batch size is reached then
   * a write should happen
   */
  def shouldWrite(): Boolean = {
    if (!writeRequired)
      require(size <= batchSize)
    writeRequired
  }

  final def add(write: AwaitingWrite): Buffer = {
    val firstTimeBucket = write.events.head._1.timeBucket
    val lastTimeBucket = write.events.last._1.timeBucket
    if (firstTimeBucket != lastTimeBucket) {
      // this write needs broken up as it spans multiple time buckets
      val (first, rest) = write.events.partition {
        case (serialized, _) => serialized.timeBucket == firstTimeBucket
      }
      add(AwaitingWrite(first, OptionVal.None)).add(AwaitingWrite(rest, write.ack))
    } else {
      // common case
      val newSize = size + write.events.size
      if (writeRequired) {
        // add them to pending, any time bucket changes will be detected later
        copy(size = newSize, pending = pending :+ write)
      } else if (nextBatch.headOption.exists(oldestEvent =>
                   UUIDComparator.comparator
                     .compare(write.events.head._1.timeUuid, oldestEvent.events.head._1.timeUuid) < 0)) {
        // rare case where events have been received out of order, just re-build the buffer
        require(pending.isEmpty)
        val allWrites = (nextBatch :+ write).sortBy(_.events.head._1.timeUuid)(timeUuidOrdering)
        rebuild(allWrites)
      } else if (nextBatch.headOption.exists(_.events.head._1.timeBucket != write.events.head._1.timeBucket)) {
        // time bucket has changed
        copy(size = newSize, pending = pending :+ write, writeRequired = true)
      } else if (newSize >= batchSize) {
        require(pending.isEmpty, "Pending should be empty if write not required")
        // does the new write need broken up?
        if (newSize > batchSize) {
          val toAdd = batchSize - size
          val (forNextWrite, forPending) = write.events.splitAt(toAdd)
          copy(
            size = newSize,
            nextBatch = nextBatch :+ AwaitingWrite(forNextWrite, OptionVal.None),
            pending = Vector(AwaitingWrite(forPending, write.ack)),
            writeRequired = true)
        } else {
          copy(size = newSize, nextBatch = nextBatch :+ write, writeRequired = true)
        }
      } else {
        copy(size = size + write.events.size, nextBatch = nextBatch :+ write)
      }
    }
  }

  private def rebuild(writes: Vector[AwaitingWrite]): Buffer = {
    var buffer = Buffer.empty(batchSize)
    var i = 0
    while (!buffer.shouldWrite() && i < writes.size) {
      buffer = buffer.add(writes(i))
      i += 1
    }
    //       pending may have one in it as the last one may have been a time bucket change rather than bach full
    val done = buffer.copy(pending = buffer.pending ++ writes.drop(i))
    done
  }

  final def addPending(write: AwaitingWrite): Buffer = {
    copy(size = size + write.events.size, pending = pending :+ write)
  }

  def writeComplete(): Buffer = {
    // this could be more efficient by adding until a write is required but this is simpler and
    // pending is expected to be small unless the database is falling behind
    rebuild(pending)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object Buffer {
  def empty(batchSize: Int): Buffer = {
    require(batchSize > 0)
    Buffer(batchSize, 0, Vector.empty, Vector.empty, writeRequired = false)
  }
}
