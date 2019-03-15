/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

package object journal {
  def partitionNr(sequenceNr: Long, partitionSize: Long): Long =
    (sequenceNr - 1L) / partitionSize
}
