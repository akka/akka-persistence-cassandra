/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import akka.Done
import akka.actor.Props
import akka.event.Logging
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.cassandra.{ CassandraSpec, Persister }
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ ExecutionContext, Future }
import Persister._

object CassandraSnapshotCleanupSpec {
  val config = ConfigFactory.parseString(
    """
      akka.loglevel = INFO
    """.stripMargin)
}

class CassandraSnapshotCleanupSpec extends CassandraSpec(CassandraSnapshotCleanupSpec.config) {

  private val log = Logging(system, getClass)
  val snapshotCleanup = new CassandraSnapshotCleanup {
    override def snapshotConfig: CassandraSnapshotStoreConfig =
      new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig("cassandra-snapshot-store"))
    override val session: CassandraSession = new CassandraSession(
      system,
      snapshotConfig.sessionProvider,
      snapshotConfig.sessionSettings,
      system.dispatcher,
      log,
      systemName,
      init = _ => Future.successful(Done))
    override implicit val ec: ExecutionContext = system.dispatcher
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
