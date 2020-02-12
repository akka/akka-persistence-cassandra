/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import scala.concurrent.duration.FiniteDuration
import akka.util.JavaDurationConverters._
import scala.concurrent.duration._

class CassandraWriteSettings private (val parallelism: Int, val maxBatchSize: Int, val maxBatchWait: FiniteDuration) {
  require(maxBatchSize > 0, s"Invalid value for maxBatchSize: $maxBatchSize. It should be > 0.")

  /**
   * WARNING: setting a write parallelism other than 1 will lead to out-of-order updates
   */
  def withParallelism(value: Int): CassandraWriteSettings = copy(parallelism = value)

  def withMaxBatchSize(maxBatchSize: Int): CassandraWriteSettings =
    copy(maxBatchSize = maxBatchSize)

  def withMaxBatchWait(maxBatchWait: FiniteDuration): CassandraWriteSettings =
    copy(maxBatchWait = maxBatchWait)

  def withMaxBatchWait(maxBatchWait: java.time.Duration): CassandraWriteSettings =
    copy(maxBatchWait = maxBatchWait.asScala)

  private def copy(
      parallelism: Int = parallelism,
      maxBatchSize: Int = maxBatchSize,
      maxBatchWait: FiniteDuration = maxBatchWait) =
    new CassandraWriteSettings(parallelism, maxBatchSize, maxBatchWait)

  override def toString: String =
    "CassandraWriteSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxBatchWait=$maxBatchWait)"

}

object CassandraWriteSettings {
  val defaults: CassandraWriteSettings = new CassandraWriteSettings(1, 100, 500.millis)

  def create(): CassandraWriteSettings = defaults
  def apply(): CassandraWriteSettings = defaults
}
