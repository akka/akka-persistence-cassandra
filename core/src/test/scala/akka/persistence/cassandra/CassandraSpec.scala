/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraSpec._
import akka.persistence.cassandra.query.EventsByPersistenceIdStage
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink }
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import scala.collection.immutable

object CassandraSpec {
  def getCallerName(clazz: Class[_]): String = {
    val s = (Thread.currentThread.getStackTrace map (_.getClassName) drop 1)
      .dropWhile(_ matches "(java.lang.Thread|.*\\.Abstract.*)")
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 ⇒ s
      case z  ⇒ s drop (z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def configWithKeyspace(name: String) = ConfigFactory.parseString(
    s"""
      cassandra-journal.keyspace = $name
    """
  ).withFallback(CassandraLifecycle.config)
}

abstract class CassandraSpec(config: Config) extends TestKit(ActorSystem(getCallerName(getClass), config.withFallback(configWithKeyspace(getCallerName(getClass)))))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  override def systemName = system.name
  implicit val mat = ActorMaterializer()(system)

  lazy val queries: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def eventsPayloads(pid: String): Seq[Any] =
    queries.currentEventsByPersistenceId(pid, 0, Long.MaxValue)
      .map(e => e.event)
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

  def events(pid: String): immutable.Seq[EventsByPersistenceIdStage.TaggedPersistentRepr] =
    queries.eventsByPersistenceId(pid, 0, Long.MaxValue, Long.MaxValue, 100, None, "test",
      extractor = Extractors.taggedPersistentRepr)
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

  def eventPayloadsWithTags(pid: String): immutable.Seq[(Any, Set[String])] =
    queries.eventsByPersistenceId(pid, 0, Long.MaxValue, Long.MaxValue, 100, None, "test",
      extractor = Extractors.taggedPersistentRepr)
      .map { tpr => (tpr.pr.payload, tpr.tags) }
      .toMat(Sink.seq)(Keep.right)
      .run().futureValue

}
