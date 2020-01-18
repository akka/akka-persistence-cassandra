/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import akka.Done
import akka.actor.Props
import akka.cassandra.session.scaladsl.CassandraSession
import akka.persistence.cassandra.{ CassandraSpec, Persister }
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import Persister._
import akka.actor.ExtendedActorSystem
import akka.cassandra.session.CqlSessionProvider
import akka.event.Logging

class CassandraSnapshotCleanupSpec extends CassandraSpec {

  private val configPath = "cassandra-plugin"

  private val log = Logging(system, getClass)

  val snapshotCleanup = new CassandraSnapshotCleanup {
    override def snapshotConfig: CassandraSnapshotStoreConfig =
      new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig(configPath))

    override implicit val ec: ExecutionContext = system.dispatcher

    // use separate session, not shared via CassandraSessionRegistry because init is different
    private val sessionProvider =
      CqlSessionProvider(system.asInstanceOf[ExtendedActorSystem], system.settings.config.getConfig(configPath))
    override val session: CassandraSession =
      new CassandraSession(system, sessionProvider, ec, log, systemName, init = _ => Future.successful(Done))

  }

  "Snapshot cleanup" must {
    "delete all snapshots for a given persistenceId" in {
      val pid = "pid"
      val a = system.actorOf(Props(new Persister(pid)))
      a ! Snapshot("cat")
      expectMsgType[SnapshotAck.type]

      val a2 = system.actorOf(Props(new Persister(pid)))
      a2 ! GetSnapshot
      expectMsg(Option("cat"))

      snapshotCleanup.deleteAllForPersistenceId(pid).futureValue shouldEqual Done

      val a3 = system.actorOf(Props(new Persister(pid)))
      a3 ! GetSnapshot
      expectMsg(None)
    }
  }
}
