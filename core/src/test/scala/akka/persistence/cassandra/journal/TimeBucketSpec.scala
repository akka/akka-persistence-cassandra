/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import org.scalatest.{ Matchers, WordSpec }

class TimeBucketSpec extends WordSpec with Matchers {
  "TimeBucket sizes" must {
    "support day" in {
      val epochTime = 1409545135047L
      val day = TimeBucket(epochTime, Day)
      day.startTimestamp should equal(1409529600000L)
      day.inPast should equal(true)
      day.isCurrent should equal(false)
      day.key should equal(1409529600000L)
      day.next().startTimestamp should equal(1409616000000L)
      day.next() > day should equal(true)
      day < day.next() should equal(true)
    }

    "support hour" in {
      val epochTime = 1409545135047L
      val hour = TimeBucket(epochTime, Hour)
      hour.startTimestamp should equal(1409544000000L)
      hour.inPast should equal(true)
      hour.isCurrent should equal(false)
      hour.key should equal(1409544000000L)
      hour.next().startTimestamp should equal(1409547600000L)
      hour.next() > hour should equal(true)
      hour < hour.next() should equal(true)
    }

    "support minute" in {
      val epochTime = 1409545135047L
      val minute = TimeBucket(epochTime, Minute)
      minute.startTimestamp should equal(1409545080000L)
      minute.inPast should equal(true)
      minute.isCurrent should equal(false)
      minute.key should equal(1409545080000L)
      minute.next().startTimestamp should equal(1409545140000L)
      minute.next() > minute should equal(true)
      minute < minute.next() should equal(true)
    }
  }
}
