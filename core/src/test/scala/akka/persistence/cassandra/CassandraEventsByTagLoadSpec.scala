/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ NoOffset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration._
import scala.util.Try

object CassandraEventsByTagLoadSpec {

  val keyspaceName = "CassandraEventsByTagLoadSpec"

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = INFO
       |}
       |cassandra-journal {
       |  keyspace = $keyspaceName
       |  log-queries = off
       |  events-by-tag {
       |     max-message-batch-size = 200
       |     bucket-size = "Minute"
       |  }
       |}
       |cassandra-snapshot-store.keyspace=CassandraEventsByTagLoadSpecSnapshot
       |cassandra-query-journal = {
       |  first-time-bucket = "20171105"
       |}
       |akka.actor.serialize-messages=off
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)
}

class CassandraEventsByTagLoadSpec extends TestKit(ActorSystem("CassandraEventsByTagLoadSpec", CassandraEventsByTagLoadSpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  override def systemName = "CassandraEventsByTagLoadSpec"

  implicit val materialiser = ActorMaterializer()(system)
  implicit override val patienceConfig = PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Seconds))

  val nrPersistenceIds = 100
  val eventTags = Set("orange", "green", "red")
  val messagesPerPersistenceId = 1000
  val veryLongWait = 60.seconds

  "Events by tag" must {
    "Events should come in sequence number order for each persistence id" in {
      val refs: Vector[ActorRef] = (1 to nrPersistenceIds).map(i => {
        system.actorOf(Props(classOf[TestTaggingActor], s"p-$i", eventTags))
      }).toVector

      system.log.info("sending first half of events")
      1L until (messagesPerPersistenceId / 2) foreach { i =>
        refs.foreach { ref =>
          ref ! s"e-$i"
        }
      }

      // Make sure we go to at least two buckets
      system.log.info("sleeping for one minute")
      Thread.sleep(1000 * 60)

      system.log.info("second load of events")
      (messagesPerPersistenceId / 2) to messagesPerPersistenceId foreach { i =>
        refs.foreach { ref =>
          ref ! s"e-$i"
        }
      }

      val readJournal =
        PersistenceQuery(system).readJournalFor[CassandraReadJournal]("cassandra-query-journal")

      eventTags.foreach({ tag =>
        try {
          validateTagStream(readJournal)(tag)
        } catch {
          case e: Throwable =>
            system.log.error("IMPORTANT:: Failed, retrying to see if it was eventual consistency")
            system.log.error("IMPORTANT:: " + e.getMessage)
            validateTagStream(readJournal)(tag)
            system.log.info("IMPORTANT:: Passed second time")
            throw new RuntimeException("Only passed the second time")
        }

      })
    }
  }

  private def validateTagStream(readJournal: CassandraReadJournal)(tag: String): Unit = {
    system.log.info(s"Validating tag $tag")
    val probe = readJournal.eventsByTag("orange", NoOffset).toMat(TestSink.probe)(Keep.right).run
    var sequenceNrsPerPid = Map[String, Long]()
    var allReceived = Map.empty[String, List[Long]].withDefaultValue(List.empty[Long])
    probe.request(messagesPerPersistenceId * nrPersistenceIds)

    (1 to (messagesPerPersistenceId * nrPersistenceIds)) foreach { i: Int =>
      val event = try {
        probe.expectNext(veryLongWait)
      } catch {
        case e: AssertionError =>
          system.log.error(e, "Failed to get event")
          allReceived.filter(_._2.size != messagesPerPersistenceId).foreach(p => system.log.info("{}", p))
          throw e
      }

      allReceived += (event.persistenceId -> (event.sequenceNr :: allReceived(event.persistenceId)))
      var fail = false
      sequenceNrsPerPid.get(event.persistenceId) match {
        case Some(currentSeqNr) =>
          if (event.sequenceNr != currentSeqNr + 1) {
            fail = true
            system.log.error(s"Out of order sequence nrs for pid ${event.persistenceId}. This was event nr [$i]. Expected ${currentSeqNr + 1}, got: ${event.sequenceNr}")
          }
          sequenceNrsPerPid += (event.persistenceId -> event.sequenceNr)
        case None =>
          event.sequenceNr should equal(1)
          sequenceNrsPerPid += (event.persistenceId -> event.sequenceNr)
      }
    }
  }

  override protected def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .addContactPoint("localhost")
      .build()
    Try(cluster.connect().execute(s"drop keyspace ${CassandraEventsByTagLoadSpec.keyspaceName}"))
    cluster.close()
  }
}
