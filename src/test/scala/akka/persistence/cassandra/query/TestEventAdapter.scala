/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import akka.persistence.journal.{ Tagged, EventSeq, EventAdapter }

sealed trait TestEvent[T] {
  def value: T
}

class TestEventAdapter(system: ExtendedActorSystem) extends EventAdapter {

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case e: String if e.startsWith("tagged:") => Tagged(e.stripPrefix("tagged:"), Set("red"))
    case e                                    => e
  }

  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case e: String if e.contains(":") => e.split(":").toList match {
      case "dropped" :: _ :: Nil            => EventSeq.empty
      case "duplicated" :: x :: Nil         => EventSeq(x, x)
      case "prefixed" :: prefix :: x :: Nil => EventSeq.single(s"$prefix-$x")
    }
    case _ => EventSeq.single(event)
  }
}
