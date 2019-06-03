/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra

import akka.util.JavaDurationConverters._
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

final class CassandraSessionSettings private (
    val fetchSize: Int,
    val readConsistency: ConsistencyLevel,
    val writeConsistency: ConsistencyLevel,
    val connectionRetries: Int,
    val connectionRetryDelay: FiniteDuration) {

  def withFetchSize(value: Int): CassandraSessionSettings = copy(fetchSize = value)
  def withReadConsistency(value: ConsistencyLevel): CassandraSessionSettings = copy(readConsistency = value)
  def withWriteConsistency(value: ConsistencyLevel): CassandraSessionSettings = copy(writeConsistency = value)
  def withConnectionRetries(value: Int): CassandraSessionSettings = copy(connectionRetries = value)

  /** Scala API */
  def withConnectionRetryDelay(value: FiniteDuration): CassandraSessionSettings = copy(connectionRetryDelay = value)

  /** Java API */
  def withConnectionRetryDelay(value: java.time.Duration): CassandraSessionSettings =
    copy(connectionRetryDelay = value.asScala)

  private def copy(
      fetchSize: Int = fetchSize,
      readConsistency: ConsistencyLevel = readConsistency,
      writeConsistency: ConsistencyLevel = writeConsistency,
      connectionRetries: Int = connectionRetries,
      connectionRetryDelay: FiniteDuration = connectionRetryDelay): CassandraSessionSettings =
    new CassandraSessionSettings(
      fetchSize = fetchSize,
      readConsistency = readConsistency,
      writeConsistency = writeConsistency,
      connectionRetries = connectionRetries,
      connectionRetryDelay = connectionRetryDelay)

  override def toString =
    "CassandraSessionSettings(" +
    s"fetchSize=$fetchSize," +
    s"readConsistency=$readConsistency," +
    s"writeConsistency=$writeConsistency," +
    s"connectionRetries=$connectionRetries," +
    s"connectionRetryDelay=${connectionRetryDelay.toCoarsest}" +
    ")"
}

object CassandraSessionSettings {

  /**
   * Reads from the given config.
   */
  def apply(config: Config): CassandraSessionSettings = {
    val fetchSize = config.getInt("max-result-size")
    val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
    val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))
    val connectionRetries: Int = config.getInt("connect-retries")
    val connectionRetryDelay: FiniteDuration = config.getDuration("connect-retry-delay").asScala

    new CassandraSessionSettings(fetchSize, readConsistency, writeConsistency, connectionRetries, connectionRetryDelay)
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config): CassandraSessionSettings = apply(c)
}
