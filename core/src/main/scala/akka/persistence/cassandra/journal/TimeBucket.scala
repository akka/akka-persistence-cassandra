/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.annotation.InternalApi
import akka.util.HashCode
import com.datastax.oss.driver.api.core.uuid.Uuids

@InternalApi private[akka] object TimeBucket {

  def apply(timeuuid: UUID, bucketSize: BucketSize): TimeBucket =
    apply(Uuids.unixTimestamp(timeuuid), bucketSize)

  def apply(epochTimestamp: Long, bucketSize: BucketSize): TimeBucket =
    // round down to bucket size so the times are deterministic
    new TimeBucket(roundDownBucketSize(epochTimestamp, bucketSize), bucketSize)

  private def roundDownBucketSize(time: Long, bucketSize: BucketSize): Long = {
    val key: Long = time / bucketSize.durationMillis
    key * bucketSize.durationMillis
  }
}

@InternalApi private[akka] final class TimeBucket private (val key: Long, val bucketSize: BucketSize) {
  def inPast: Boolean =
    key < TimeBucket.roundDownBucketSize(System.currentTimeMillis(), bucketSize)

  def isCurrent: Boolean = {
    val now = System.currentTimeMillis()
    now >= key && now < (key + bucketSize.durationMillis)
  }

  def within(uuid: UUID): Boolean = {
    val when = Uuids.unixTimestamp(uuid)
    when >= key && when < (key + bucketSize.durationMillis)
  }

  def next(): TimeBucket =
    new TimeBucket(key + bucketSize.durationMillis, bucketSize)

  def previous(steps: Int): TimeBucket =
    if (steps == 0) this
    else new TimeBucket(key - steps * bucketSize.durationMillis, bucketSize)

  def >(other: TimeBucket): Boolean =
    key > other.key

  def <(other: TimeBucket): Boolean =
    key < other.key

  override def equals(other: Any): Boolean = other match {
    case that: TimeBucket =>
      key == that.key &&
      bucketSize == that.bucketSize
    case _ => false
  }

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, key)
    result = HashCode.hash(result, bucketSize)
    result
  }

  import akka.persistence.cassandra._

  override def toString =
    s"TimeBucket($key, $bucketSize, inPast: $inPast, currentBucket: $isCurrent. time: ${formatUnixTime(key)} )"
}
