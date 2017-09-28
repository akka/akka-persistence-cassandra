/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import scala.collection.immutable
import akka.actor.Props
import akka.persistence.PersistentActor

object TestActor {
  def props(persistenceId: String): Props =
    Props(new TestActor(persistenceId))

  final case class PersistAll(events: immutable.Seq[String])
}

class TestActor(override val persistenceId: String) extends PersistentActor {

  val receiveRecover: Receive = {
    case evt: String =>
  }

  val receiveCommand: Receive = {
    case cmd: String =>
      persist(cmd) { evt =>
        sender() ! evt + "-done"
      }
    case TestActor.PersistAll(events) =>
      val size = events.size
      val handler = {
        var count = 0
        (evt: String) => {
          count += 1
          if (count == size)
            sender() ! "PersistAll-done"
        }
      }
      persistAll(events)(handler)
  }
}
