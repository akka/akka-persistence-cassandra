package akka.persistence.cassandra.example

import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.ActorRef

object ReadSideTopic {

  final case class ReadSideMetrics(count: Long, maxValue: Long, p99: Long, p50: Long)

  def init(context: ActorContext[_]): ActorRef[Topic.Command[ReadSideMetrics]] = {
    context.spawn(Topic[ReadSideMetrics]("read-side-metrics"), "read-side-metrics")
  }

}
