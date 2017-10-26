/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.util.UUID

import akka.annotation.InternalApi
import com.datastax.driver.core.utils.UUIDs

@InternalApi private[akka] object TimeBucket {

  def apply(timeuuid: UUID, bucketSize: BucketSize = Day): TimeBucket =
    apply(UUIDs.unixTimestamp(timeuuid), bucketSize)

  def apply(epochTimestamp: Long, bucketSize: BucketSize): TimeBucket = {
    // round down to bucket size so the times are deterministic
    new TimeBucket(roundDownBucketSize(epochTimestamp, bucketSize), bucketSize)
  }

  private def roundDownBucketSize(time: Long, bucketSize: BucketSize): Long = {
    val key: Long = time / bucketSize.durationMillis
    key * bucketSize.durationMillis
  }
}

@InternalApi private[akka] final class TimeBucket private (val startTimestamp: Long, bucketSize: BucketSize) {
  val key: Long = startTimestamp

  def inPast: Boolean =
    startTimestamp < TimeBucket.roundDownBucketSize(System.currentTimeMillis(), bucketSize)

  def isCurrent: Boolean = {
    val now = System.currentTimeMillis()
    now >= startTimestamp && now < (startTimestamp + bucketSize.durationMillis)
  }

  def next(): TimeBucket =
    new TimeBucket(startTimestamp + bucketSize.durationMillis, bucketSize)

  def previous(steps: Int): TimeBucket =
    if (steps == 0) this
    else new TimeBucket(startTimestamp - steps * bucketSize.durationMillis, bucketSize)

  def >(other: TimeBucket): Boolean = {
    startTimestamp > other.startTimestamp
  }

  def <(other: TimeBucket): Boolean = {
    startTimestamp < other.startTimestamp
  }
  override def toString = s"TimeBucket($key, $startTimestamp, $inPast, $isCurrent)"
}
