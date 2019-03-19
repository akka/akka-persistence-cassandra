/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.format.DateTimeFormatter
import java.time.{ LocalDateTime, ZoneId, ZoneOffset }
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

package object query {

  val firstBucketFormat = "yyyyMMdd'T'HH:mm"
  val firstBucketFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(firstBucketFormat).withZone(ZoneOffset.UTC)

  def uuid(timestamp: Long): UUID = {
    def makeMsb(time: Long): Long = {
      // copied from UUIDs.makeMsb
      // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
      val uuidEpoch = LocalDateTime.of(1582, 10, 15, 0, 0).atZone(ZoneId.of("GMT-0")).toInstant.toEpochMilli
      val timestamp = (time - uuidEpoch) * 10000

      var msb = 0L
      msb |= (0X00000000FFFFFFFFL & timestamp) << 32
      msb |= (0X0000FFFF00000000L & timestamp) >>> 16
      msb |= (0X0FFF000000000000L & timestamp) >>> 48
      msb |= 0X0000000000001000L // sets the version to 1.
      msb
    }

    val now = UUIDs.timeBased()
    new UUID(makeMsb(timestamp), now.getLeastSignificantBits)
  }
}
