/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import akka.actor.Props
import akka.persistence.PersistentActor

object TestActor {
  def props(persistenceId: String): Props =
    Props(new TestActor(persistenceId))
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
  }
}
