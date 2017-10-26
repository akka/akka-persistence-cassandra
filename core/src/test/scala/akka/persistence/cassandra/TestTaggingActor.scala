/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.cassandra.TestTaggingActor.Ack
import akka.persistence.journal.Tagged

object TestTaggingActor {
  case object Ack

  def props(pId: String, tags: Set[String]): Props =
    Props(classOf[TestTaggingActor], pId, tags)
}

class TestTaggingActor(val persistenceId: String, tags: Set[String]) extends PersistentActor {
  def receiveRecover: Receive = processEvent

  def receiveCommand: Receive = {
    case event: String =>
      persist(Tagged(event, tags)) { e =>
        processEvent(e)
        sender() ! Ack
      }
  }

  def processEvent: Receive = {
    case _ =>
  }
}
