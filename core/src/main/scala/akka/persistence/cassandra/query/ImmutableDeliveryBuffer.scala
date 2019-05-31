/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.stream.actor.ActorPublisher
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait ImmutableDeliveryBuffer[T] { _: ActorPublisher[T] =>

  def deliverBuf(buf: Vector[T]): Vector[T] =
    if (buf.nonEmpty && totalDemand > 0) {
      if (buf.size == 1) {
        onNext(buf.head)
        Vector.empty[T]
      } else if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        use.foreach(onNext)
        keep
      } else {
        buf.foreach(onNext)
        Vector.empty[T]
      }
    } else {
      buf
    }
}
