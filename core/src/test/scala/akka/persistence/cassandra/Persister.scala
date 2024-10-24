/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorRef
import akka.persistence._
import akka.persistence.cassandra.Persister._

object Persister {
  case class CrapEvent(n: Int)
  case class Snapshot(s: Any)
  case object GetSnapshot
  case object SnapshotAck
  case object SnapshotNack
  case class DeleteSnapshot(sequenceNr: Long)
  case class DeleteSnapshots(sequenceNr: Long)
}

class Persister(override val persistenceId: String, probe: Option[ActorRef] = None) extends PersistentActor {
  def this(pid: String, probe: ActorRef) = this(pid, Some(probe))

  var snapshot: Option[Any] = None
  var snapshotAck: Option[ActorRef] = None
  var deleteSnapshotAck: Option[ActorRef] = None

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
    case DeleteSnapshot(nr) =>
      deleteSnapshot(nr)
      deleteSnapshotAck = Some(sender())
    case DeleteSnapshots(nr) =>
      deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = nr))
      deleteSnapshotAck = Some(sender())
    case DeleteSnapshotsSuccess(cri) =>
      deleteSnapshotAck.foreach(_ ! cri)
    case DeleteSnapshotSuccess(cri) =>
      deleteSnapshotAck.foreach(_ ! cri)
      deleteSnapshotAck = None
    case DeleteSnapshotFailure(m, c) =>
      println(s"$m -> $c")
    case msg =>
      persist(msg) { _ =>
        probe.foreach(_ ! msg)
      }
  }

  override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit =
    probe.foreach(_ ! cause)
}
