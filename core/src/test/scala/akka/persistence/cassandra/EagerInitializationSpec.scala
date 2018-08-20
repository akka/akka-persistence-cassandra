/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ ActorSystem, PoisonPill }
import akka.persistence.cassandra.TestTaggingActor.Ack
import com.datastax.driver.core.Session
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.{ ExecutionContext, Future }

object TestSessionProvider {
}

class TestSessionProvider(as: ActorSystem, config: Config) extends ConfigSessionProvider(as, config) {
  override def connect()(implicit ec: ExecutionContext): Future[Session] = {
    val e = new Exception()
    if (e.getStackTrace.exists(s => { s.toString.contains("eventsByPersistenceId") })) {
      as.log.error(e, "Stack trace for your debugging pleasure")
      throw e
    }
    super.connect()
  }
}

object EagerInitializationSpec {
  val config = ConfigFactory.parseString(
    """
    cassandra-journal {
      session-provider = akka.persistence.cassandra.TestSessionProvider
    }
    """)
}

class EagerInitializationSpec extends CassandraSpec(EagerInitializationSpec.config) {

  "Eager initialization" in {
    val p = system.actorOf(TestTaggingActor.props("p1"))
    p ! "persist"
    expectMsg(Ack)
    p ! PoisonPill
    val p2 = system.actorOf(TestTaggingActor.props("p1"))
    p2 ! "persist"
    expectMsg(Ack)
  }
}
