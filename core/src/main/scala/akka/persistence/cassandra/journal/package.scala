/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

package object journal {
  def partitionNr(sequenceNr: Long, partitionSize: Long): Long =
    (sequenceNr - 1L) / partitionSize
}
