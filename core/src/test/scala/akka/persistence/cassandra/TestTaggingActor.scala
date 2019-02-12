/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted, SaveSnapshotSuccess}
import akka.persistence.journal.Tagged

object TestTaggingActor {
  case object Ack
  case object DoASnapshotPlease
  case object SnapShotAck
  case object Stop

  def props(pId: String, tags: Set[String] = Set(), probe: Option[ActorRef] = None): Props =
    Props(new TestTaggingActor(pId, tags, probe))
}

class TestTaggingActor(val persistenceId: String, tags: Set[String], probe: Option[ActorRef])
    extends PersistentActor
    with ActorLogging {
  import TestTaggingActor._

  def receiveRecover: Receive = {
    case RecoveryCompleted =>
      probe.foreach(_ ! RecoveryCompleted)
    case _ =>
  }

  def receiveCommand: Receive = normal

  def normal: Receive = {
    case event: String =>
      log.debug("Persisting {}", event)
      persist(Tagged(event, tags)) { e =>
        processEvent(e)
        sender() ! Ack
      }
    case DoASnapshotPlease =>
      saveSnapshot("i don't have any state :-/")
      context.become(waitingForSnapshot(sender()))
    case Stop =>
      context.stop(self)

  }

  def waitingForSnapshot(who: ActorRef): Receive = {
    case SaveSnapshotSuccess(_) =>
      who ! SnapShotAck
      context.become(normal)
  }

  def processEvent: Receive = {
    case _ =>
  }
}
