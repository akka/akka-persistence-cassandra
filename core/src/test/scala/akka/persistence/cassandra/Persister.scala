/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorRef
import akka.persistence.{ PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer }
import akka.persistence.cassandra.Persister._

object Persister {
  case class CrapEvent(n: Int)
  case class Snapshot(s: Any)
  case object GetSnapshot
  case object SnapshotAck
  case object SnapshotNack
}

class Persister(override val persistenceId: String, probe: Option[ActorRef] = None) extends PersistentActor {
  def this(pid: String, probe: ActorRef) = this(pid, Some(probe))

  var snapshot: Option[Any] = None
  var snapshotAck: Option[ActorRef] = None

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, s) =>
      snapshot = Some(s)
    case msg => probe.foreach(_ ! msg)
  }
  override def receiveCommand: Receive = {
    case GetSnapshot =>
      sender() ! snapshot
    case Snapshot(s) =>
      snapshotAck = Some(sender())
      saveSnapshot(s)
    case SaveSnapshotSuccess(_) =>
      snapshotAck.foreach(_ ! SnapshotAck)
      snapshotAck = None
    case SaveSnapshotFailure(_, _) =>
      snapshotAck.foreach(_ ! SnapshotNack)
      snapshotAck = None
    case msg => persist(msg) { _ =>
      probe.foreach(_ ! msg)
    }
  }

  override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
    probe.foreach(_ ! cause)
  }
}

