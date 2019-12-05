/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.session.javadsl

import java.util.Optional

import akka.Done
import akka.cassandra.session.javadsl.CassandraSession
import akka.cassandra.session.DefaultSessionProvider
import akka.event.Logging
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.stream.testkit.scaladsl.TestSink
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder
import com.datastax.oss.driver.api.core.cql.BatchType
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

object CassandraSessionSpec {

  lazy val config = ConfigFactory.parseString(s"""
      cassandra-journal.keyspace=CassandraSessionSpec
    """).withFallback(CassandraLifecycle.config)

}

class CassandraSessionSpec extends CassandraSpec(CassandraSessionSpec.config) {
  import system.dispatcher

  val log = Logging.getLogger(system, this.getClass)

  lazy val session: CassandraSession = {
    val cfg = system.settings.config.withFallback(system.settings.config.getConfig("cassandra-journal"))
    new CassandraSession(
      system,
      new DefaultSessionProvider(system, cfg),
      system.dispatcher,
      log,
      "CassandraSessionSpec-metrics",
      (s: CqlSession) => s.executeAsync(s"USE ${cfg.getString("keyspace")};").toScala.map(_ => Done.getInstance).toJava)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
    insertTestData()
  }

  def createTable(): Unit =
    Await.ready(
      session.executeDDL(s"""
      CREATE TABLE IF NOT EXISTS testcounts (
        partition text,
        key text,
        count bigint,
        PRIMARY KEY (partition, key))
        """).toScala,
      15.seconds)

  def insertTestData(): Unit = {
    val batch = new BatchStatementBuilder(BatchType.UNLOGGED)
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'a', 1);"))
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'b', 2);"))
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'c', 3);"))
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('A', 'd', 4);"))
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('B', 'e', 5);"))
    batch.addStatement(
      SimpleStatement.newInstance("INSERT INTO testcounts (partition, key, count) VALUES ('B', 'f', 6);"))
    Await.ready(session.executeWriteBatch(batch.build()).toScala, 10.seconds)
  }

  "CassandraSession" must {

    "select prepared Statement[_]as Source" in {
      val stmt = Await.result(session.prepare("SELECT count FROM testcounts WHERE partition = ?").toScala, 5.seconds)
      val bound = stmt.bind("A")
      val rows = session.select(bound).asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10).expectNextUnordered(1L, 2L, 3L, 4L).expectComplete()
      }
    }

    "select and bind as Source" in {
      val rows = session.select("SELECT count FROM testcounts WHERE partition = ?", "B").asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10).expectNextUnordered(5L, 6L).expectComplete()
      }
    }

    "selectAll and bind" in {
      val rows =
        Await.result(session.selectAll("SELECT count FROM testcounts WHERE partition = ?", "A").toScala, 5.seconds)
      rows.asScala.map(_.getLong("count")).toSet should ===(Set(1L, 2L, 3L, 4L))
    }

    "selectAll empty" in {
      val rows =
        Await.result(session.selectAll("SELECT count FROM testcounts WHERE partition = ?", "X").toScala, 5.seconds)
      rows.isEmpty should ===(true)
    }

    "selectOne and bind" in {
      val row = Await.result(
        session.selectOne("SELECT count FROM testcounts WHERE partition = ? and key = ?", "A", "b").toScala,
        5.seconds)
      row.get.getLong("count") should ===(2L)
    }

    "selectOne empty" in {
      val row = Await.result(
        session.selectOne("SELECT count FROM testcounts WHERE partition = ? and key = ?", "A", "x").toScala,
        5.seconds)
      row should be(Optional.empty())
    }

    "create indexes" in {
      Await.result(session.executeDDL("CREATE INDEX IF NOT EXISTS count_idx ON testcounts(count)").toScala, 5.seconds)
      val row = Await
        .result(
          session
            .selectOne("SELECT * FROM system_schema.indexes WHERE table_name = ? ALLOW FILTERING", "testcounts")
            .toScala,
          5.seconds)
        .asScala
      row.map(index => index.getString("table_name") -> index.getString("index_name")) should be(
        Some("testcounts" -> "count_idx"))
    }
  }

}
