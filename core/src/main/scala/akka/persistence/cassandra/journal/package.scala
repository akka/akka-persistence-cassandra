/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra

import akka.annotation.InternalApi

package object journal {

  /** INTERNAL API */
  @InternalApi private[akka] def partitionNr(sequenceNr: Long, partitionSize: Long): Long =
    (sequenceNr - 1L) / partitionSize
}
