package akka.persistence.cassandra.example

import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.cassandra.example.LoadGenerator.Start
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry

import scala.concurrent.Await
import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {

    ActorSystem(Behaviors.setup[SelfUp] {
      ctx =>
        val readSettings = ReadSide.Settings(ctx.system.settings.config.getConfig("cassandra.example"))
        val writeSettings = ConfigurablePersistentActor.Settings(readSettings.nrTags)
        val loadSettings = LoadGenerator.Settings(ctx.system.settings.config.getConfig("cassandra.example"))

        AkkaManagement(ctx.system).start()
        ClusterBootstrap(ctx.system).start()
        val cluster = Cluster(ctx.system)
        cluster.subscriptions ! Subscribe(ctx.self, classOf[SelfUp])

        val topic = ReadSideTopic.init(ctx)

        if (cluster.selfMember.hasRole("read")) {
          val session = CassandraSessionRegistry(ctx.system).sessionFor("akka.persistence.cassandra")
          val offsetTableStmt =
            """
              CREATE TABLE IF NOT EXISTS akka.offsetStore (
                eventProcessorId text,
                tag text,
                timeUuidOffset timeuuid,
                PRIMARY KEY (eventProcessorId, tag)
              )
           """

          Await.ready(session.executeDDL(offsetTableStmt), 30.seconds)
        }

        Behaviors.receiveMessage {
          case SelfUp(state) =>
            ctx.log.info(
              "Cluster member joined. Initializing persistent actors. Roles {}. Members {}",
              cluster.selfMember.roles,
              state.members)
            val ref = ConfigurablePersistentActor.init(writeSettings, ctx.system)
            if (cluster.selfMember.hasRole("read")) {
              ctx.spawnAnonymous(Reporter(topic))
            }
            ReadSide(ctx.system, topic, readSettings)
            if (cluster.selfMember.hasRole("load")) {
              ctx.log.info("Starting load generation")
              val load = ctx.spawn(LoadGenerator(loadSettings, ref), "load-generator")
              load ! Start(10.seconds)
            }
            Behaviors.empty
        }
    }, "apc-example")
  }
}
