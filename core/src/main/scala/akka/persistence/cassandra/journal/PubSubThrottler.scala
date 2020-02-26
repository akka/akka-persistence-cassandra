/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.concurrent.duration.FiniteDuration
import akka.actor.{ Actor, ActorRef }
import akka.actor.Props
import akka.annotation.InternalApi

/**
 * INTERNAL API: Proxies messages to another actor, only allowing identical messages through once every [interval].
 */
@InternalApi private[akka] class PubSubThrottler(delegate: ActorRef, interval: FiniteDuration) extends Actor {
  import PubSubThrottler._
  import context.dispatcher

  /** The messages we've already seen during this interval */
  val seen = collection.mutable.Set.empty[Any]

  /** The messages we've seen more than once during this interval, and their sender(s). */
  val repeated = collection.mutable.Map.empty[Any, Set[ActorRef]].withDefaultValue(Set.empty)

  val timer = context.system.scheduler.schedule(interval, interval, self, Tick)

  def receive = {
    case Tick =>
      for ((msg, clients) <- repeated;
           client <- clients) {
        delegate.tell(msg, client)
      }
      seen.clear()
      repeated.clear()

    case msg =>
      if (seen.contains(msg)) {
        repeated += (msg -> (repeated(msg) + sender))
      } else {
        delegate.forward(msg)
        seen += msg
      }

  }

  override def postStop() = {
    timer.cancel()
    super.postStop()
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PubSubThrottler {

  def props(delegate: ActorRef, interval: FiniteDuration): Props =
    Props(new PubSubThrottler(delegate, interval))

  private case object Tick

}
