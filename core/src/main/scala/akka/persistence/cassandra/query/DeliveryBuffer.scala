/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.stream.actor.ActorPublisher
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait DeliveryBuffer[T] { _: ActorPublisher[T] =>

  var buf = Vector.empty[T]

  def deliverBuf(): Unit =
    if (buf.nonEmpty && totalDemand > 0) {
      if (buf.size == 1) {
        // optimize for this common case
        onNext(buf.head)
        buf = Vector.empty
      } else if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        buf foreach onNext
        buf = Vector.empty
      }
    }

}
