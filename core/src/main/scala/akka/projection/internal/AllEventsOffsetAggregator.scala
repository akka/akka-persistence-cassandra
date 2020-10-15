/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

// FIXME this will be in akka-projection-core and used by the OffsetStore implementations

import scala.collection.immutable

case class AllEventsOffsetBucket(key: String, offsets: Map[String, Long])

/**
 * Used by Projections OffsetStore for `AllEventsOffset`.
 * The `initial` corresponds to the read offsets when the projection is started.
 * Each `AllEventsOffsetBucket` is stored as a row in the offset table.
 *
 * Several buckets (rows) are used because it can be many total number of pid/seqNr
 * pairs in a `AllEventsOffset`. To reduce the amount of data that is updated
 * when the offset changes it works with one primary bucket at a time. New
 * pids are added to this primary bucket. Updated pids are moved to the primary
 * bucket (removed from old bucket, added to primary).
 *
 * Assuming that recently used pids are "hot" and updated again.
 *
 * When the size of the primary bucket is 20% larger than next largest it selects
 * the smallest bucket as the new primary.
 */
class AllEventsOffsetAggregator(initial: Vector[AllEventsOffsetBucket]) {
  require(initial.size >= 2, "At least 2 buckets required.")
  private var buckets: Array[AllEventsOffsetBucket] = initial.toArray
  private var switchPrimarySize = 0

  private def initBuckets(): Unit = {
    val sorted = buckets.toVector.sortBy(_.offsets.size)
    // Smallest first, then ordered from largest to smallest.
    // It's most likely that updates are for pids that have been recently used.
    // Then we only have to save one or two buckets.
    buckets = (sorted.head +: sorted.tail.reverse).toArray
    // switch the first bucket when it is 20% larger than currently largest
    switchPrimarySize = math.max(100, (1.2 + buckets(1).offsets.size).toInt)
  }

  initBuckets()

  /**
   * Update the offset buckets.
   * @return the changed buckets that should be saved
   */
  def add(offsets: Map[String, Long]): immutable.Seq[AllEventsOffsetBucket] = {
    if (buckets(0).offsets.size >= switchPrimarySize)
      initBuckets()

    var dirty: Set[Int] = Set.empty
    offsets.foreach {
      case (pid, seqNr) =>
        indexOfBucket(pid) match {
          case -1 | 0 =>
            // add or update in first bucket
            val bucket0 = buckets(0)
            buckets(0) = bucket0.copy(offsets = bucket0.offsets.updated(pid, seqNr))
            dirty += 0
          case i =>
            // remove from old bucket
            val oldBucket = buckets(i)
            buckets(i) = oldBucket.copy(offsets = oldBucket.offsets - pid)
            dirty += i
            // add in first bucket
            val bucket0 = buckets(0)
            buckets(0) = bucket0.copy(offsets = bucket0.offsets.updated(pid, seqNr))
            dirty += 0
        }
    }

    dirty.toList.sorted.map(buckets(_))
  }

  private def indexOfBucket(pid: String): Int =
    buckets.indexWhere(_.offsets.contains(pid))

}
