/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.TestTaggingActor.Ack
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, TestTaggingActor }
import akka.persistence.query.{ PersistenceQuery, ReadJournalProvider }
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._

class JournalOverride(as: ExtendedActorSystem, config: Config) extends CassandraReadJournal(as, config) {
  override private[akka] def mapEvent(pr: PersistentRepr) =
    PersistentRepr("cat", pr.sequenceNr, pr.persistenceId, pr.manifest, pr.deleted, pr.sender, pr.writerUuid)
}

class JournalOverrideProvider(as: ExtendedActorSystem, config: Config) extends ReadJournalProvider {
  override def scaladslReadJournal() = new JournalOverride(as, config)
  override def javadslReadJournal() = null
}

object CassandraReadJournalOverrideSpec {

  val config = ConfigFactory.parseString(
    """
      cassandra-query-journal {
        class = "akka.persistence.cassandra.query.JournalOverrideProvider"
      }
    """.stripMargin).withFallback(CassandraLifecycle.config)

}

class CassandraReadJournalOverrideSpec extends CassandraSpec(CassandraReadJournalOverrideSpec.config) {

  implicit val materialiser = ActorMaterializer()

  lazy val journal =
    PersistenceQuery(system).readJournalFor[JournalOverride](
      CassandraReadJournal.Identifier)

  "Cassandra read journal override" must {
    "map events" in {
      val pid = "p1"
      val p1 = system.actorOf(TestTaggingActor.props(pid))
      p1 ! "not a cat"
      expectMsg(Ack)

      val currentEvents = journal.currentEventsByPersistenceId(pid, 0, Long.MaxValue)
      val currentProbe = currentEvents.map(_.event.toString).runWith(TestSink.probe[String])
      currentProbe.request(2)
      currentProbe.expectNext("cat")
      currentProbe.expectComplete()

      val liveEvents = journal.eventsByPersistenceId(pid, 0, Long.MaxValue)
      val liveProbe = liveEvents.map(_.event.toString).runWith(TestSink.probe[String])
      liveProbe.request(2)
      liveProbe.expectNext("cat")
      liveProbe.expectNoMessage(100.millis)
      liveProbe.cancel()

      val internalEvents = journal.eventsByPersistenceIdWithControl(pid, 0, Long.MaxValue, None)
      val internalProbe = internalEvents.map(_.event.toString).runWith(TestSink.probe[String])
      internalProbe.request(2)
      internalProbe.expectNext("cat")
      liveProbe.expectNoMessage(100.millis)
      liveProbe.cancel()
    }
  }
}
