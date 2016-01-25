/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.concurrent.duration.FiniteDuration

import akka.actor.{ Actor, ActorRef }

/**
 * Proxies messages to another actor, only allowing identical messages through once every [interval].
 */
private[journal] class PubSubThrottler(delegate: ActorRef, interval: FiniteDuration) extends Actor {
  import PubSubThrottler._
  import context.dispatcher

  /** The messages we've already seen during this interval */
  val seen = collection.mutable.Set.empty[Any]

  /** The messages we've seen more than once during this interval, and their sender(s). */
  val repeated = collection.mutable.Map.empty[Any, Set[ActorRef]].withDefaultValue(Set.empty)

  val timer = context.system.scheduler.schedule(interval, interval, self, Tick)

  def receive = {
    case Tick =>
      for (
        (msg, clients) <- repeated;
        client <- clients
      ) {
        delegate.tell(msg, client)
      }
      seen.clear()
      repeated.clear()

    case msg =>
      if (seen.contains(msg)) {
        repeated += (msg -> (repeated(msg) + sender))
      } else {
        delegate forward msg
        seen += msg
      }

  }

  override def postStop() = {
    timer.cancel()
    super.postStop()
  }
}

private[journal] object PubSubThrottler {
  private case object Tick

}
