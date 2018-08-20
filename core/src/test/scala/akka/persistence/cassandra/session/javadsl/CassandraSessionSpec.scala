/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.session.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec, ListenableFutureConverter, SessionProvider }
import akka.stream.testkit.scaladsl.TestSink
import com.datastax.driver.core.{ BatchStatement, Session, SimpleStatement }
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

object CassandraSessionSpec {

  lazy val config = ConfigFactory.parseString(
    s"""
      akka.loglevel = INFO
      cassandra-journal.keyspace=CassandraSessionSpec

      test-cassandra-session-config {
        max-result-size = 2
      }
    """).withFallback(CassandraLifecycle.config)

}

class CassandraSessionSpec extends CassandraSpec(CassandraSessionSpec.config) {
  import system.dispatcher

  val log = Logging.getLogger(system, this.getClass)

  lazy val session: CassandraSession = {
    val cfg = system.settings.config.getConfig("test-cassandra-session-config")
      .withFallback(system.settings.config.getConfig("cassandra-journal"))
    new CassandraSession(
      system,
      SessionProvider(system.asInstanceOf[ExtendedActorSystem], cfg),
      CassandraSessionSettings(cfg),
      system.dispatcher,
      log,
      "CassandraSessionSpec-metrics",
      new java.util.function.Function[Session, CompletionStage[Done]] {
        override def apply(s: Session): CompletionStage[Done] = {
          s.executeAsync(s"USE ${cfg.getString("keyspace")};").asScala.map(_ => Done.getInstance).toJava
        }
      })
  }

  override def beforeAll: Unit = {
    super.beforeAll()
    createTable()
    insertTestData()
  }

  def createTable(): Unit = {
    Await.ready(session.executeCreateTable(s"""
      CREATE TABLE IF NOT EXISTS testcounts (
        partition text,
        key text,
        count bigint,
        PRIMARY KEY (partition, key))
        """).toScala, 15.seconds)
  }

  def insertTestData(): Unit = {
    val batch = new BatchStatement
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'a', 1);"))
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'b', 2);"))
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'c', 3);"))
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'd', 4);"))
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('B', 'e', 5);"))
    batch.add(new SimpleStatement("INSERT INTO testcounts (partition, key, count) VALUES ('B', 'f', 6);"))
    Await.ready(session.executeWriteBatch(batch).toScala, 10.seconds)
  }

  "CassandraSession" must {

    "select prepared statement as Source" in {
      val stmt = Await.result(session.prepare(
        "SELECT count FROM testcounts WHERE partition = ?").toScala, 5.seconds)
      val bound = stmt.bind("A")
      val rows = session.select(bound).asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10)
          .expectNextUnordered(1L, 2L, 3L, 4L)
          .expectComplete()
      }
    }

    "select and bind as Source" in {
      val rows = session.select("SELECT count FROM testcounts WHERE partition = ?", "B").asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10)
          .expectNextUnordered(5L, 6L)
          .expectComplete()
      }
    }

    "selectAll and bind" in {
      val rows = Await.result(session.selectAll(
        "SELECT count FROM testcounts WHERE partition = ?", "A").toScala, 5.seconds)
      rows.asScala.map(_.getLong("count")).toSet should ===(Set(1L, 2L, 3L, 4L))
    }

    "selectAll empty" in {
      val rows = Await.result(session.selectAll(
        "SELECT count FROM testcounts WHERE partition = ?", "X").toScala, 5.seconds)
      rows.isEmpty should ===(true)
    }

    "selectOne and bind" in {
      val row = Await.result(session.selectOne(
        "SELECT count FROM testcounts WHERE partition = ? and key = ?", "A", "b").toScala, 5.seconds)
      row.get.getLong("count") should ===(2L)
    }

    "selectOne empty" in {
      val row = Await.result(session.selectOne(
        "SELECT count FROM testcounts WHERE partition = ? and key = ?", "A", "x").toScala, 5.seconds)
      row should be(Optional.empty())
    }

  }

}
