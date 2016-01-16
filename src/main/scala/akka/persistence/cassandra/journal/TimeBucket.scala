/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.Instant
import java.time.ZoneOffset
import java.time.LocalDate
import com.datastax.driver.core.utils.UUIDs
import java.util.UUID

private[cassandra] object TimeBucket {
  private val timeBucketFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def apply(key: String): TimeBucket =
    apply(LocalDate.parse(key, timeBucketFormatter), key)

  def apply(timeuuid: UUID): TimeBucket =
    apply(UUIDs.unixTimestamp(timeuuid))

  def apply(epochTimestamp: Long): TimeBucket = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTimestamp), ZoneOffset.UTC)
    apply(time.toLocalDate)
  }

  def apply(day: LocalDate): TimeBucket = {
    val key = day.format(timeBucketFormatter)
    apply(day, key)
  }

}

private[cassandra] final case class TimeBucket(day: LocalDate, key: String) {

  def next(): TimeBucket =
    TimeBucket(day.plusDays(1))

  def isBefore(other: LocalDate): Boolean =
    day.isBefore(other)

  def startTimestamp: Long =
    day.atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli

  override def toString: String = key

}
