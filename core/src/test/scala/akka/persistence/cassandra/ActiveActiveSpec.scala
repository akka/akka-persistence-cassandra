/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.cassandra.ActiveActiveSpec.MyActiveActiveStringSet
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.ActiveActiveEventSourcing
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.testkit.TestProbe

object ActiveActiveSpec {

  object MyActiveActiveStringSet {
    trait Command extends CborSerializable
    case class Add(text: String) extends Command
    case class GetTexts(replyTo: ActorRef[Texts]) extends Command
    case object Stop extends Command
    case class Texts(texts: Set[String]) extends CborSerializable

    def apply(entityId: String, replicaId: ReplicaId, allReplicas: Set[ReplicaId]): Behavior[Command] =
      ActiveActiveEventSourcing.withSharedJournal(entityId, replicaId, allReplicas, CassandraReadJournal.Identifier) {
        aaContext =>
          EventSourcedBehavior[Command, String, Set[String]](
            aaContext.persistenceId,
            Set.empty[String],
            (state, command) =>
              command match {
                case Add(text) =>
                  Effect.persist(text)
                case GetTexts(replyTo) =>
                  replyTo ! Texts(state)
                  Effect.none
              },
            (state, event) => state + event).withJournalPluginId("akka.persistence.cassandra.journal")
      }
  }

}

class ActiveActiveSpec extends CassandraSpec {

  "Active active" must {
    "work with Cassandra as journal" in {
      import akka.actor.typed.scaladsl.adapter._
      val allReplicas = Set(ReplicaId("DC-A"), ReplicaId("DC-B"))
      val aProps = PropsAdapter(MyActiveActiveStringSet("id-1", ReplicaId("DC-A"), allReplicas))
      val replicaA = system.actorOf(aProps)
      val replicaB = system.actorOf(PropsAdapter(MyActiveActiveStringSet("id-1", ReplicaId("DC-B"), allReplicas)))

      replicaA ! MyActiveActiveStringSet.Add("added to a")
      replicaB ! MyActiveActiveStringSet.Add("added to b")
      val stopProbe = TestProbe()
      stopProbe.watch(replicaA)
      replicaA ! MyActiveActiveStringSet.Stop
      stopProbe.expectTerminated(replicaA)

      val restartedReplicaA = system.actorOf(aProps)
      awaitAssert {
        val probe = TestProbe()
        restartedReplicaA ! MyActiveActiveStringSet.GetTexts(probe.ref.toTyped)
        probe.expectMsgType[MyActiveActiveStringSet.Texts].texts should ===(Set("added to a", "added to b"))
      }
    }
  }

}
