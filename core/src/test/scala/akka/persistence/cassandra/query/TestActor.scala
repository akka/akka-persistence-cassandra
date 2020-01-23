/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import scala.collection.immutable
import akka.actor.Props
import akka.persistence.PersistentActor
import akka.actor.ActorRef
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.cassandra.EventWithMetaData
import akka.persistence.journal.Tagged

object TestActor {
  def props(persistenceId: String, journalId: String = "akka.persistence.cassandra.journal"): Props =
    Props(new TestActor(persistenceId, journalId))

  final case class PersistAll(events: immutable.Seq[String])
  final case class DeleteTo(seqNr: Long)
}

class TestActor(override val persistenceId: String, override val journalPluginId: String) extends PersistentActor {

  var lastDelete: ActorRef = _

  val receiveRecover: Receive = {
    case evt: String =>
  }

  val receiveCommand: Receive = {
    case cmd: String =>
      persist(cmd) { evt =>
        sender() ! evt + "-done"
      }
    case cmd: EventWithMetaData =>
      persist(cmd) { evt =>
        sender() ! s"$evt-done"
      }
    case cmd: Tagged =>
      persist(cmd) { evt =>
        val msg = s"${evt.payload}-done"
        sender() ! msg
      }

    case TestActor.PersistAll(events) =>
      val size = events.size
      val handler = {
        var count = 0
        evt: String => {
          count += 1
          if (count == size)
            sender() ! "PersistAll-done"
        }
      }
      persistAll(events)(handler)

    case TestActor.DeleteTo(seqNr) =>
      lastDelete = sender()
      deleteMessages(seqNr)

    case d: DeleteMessagesSuccess =>
      lastDelete ! d
  }
}
