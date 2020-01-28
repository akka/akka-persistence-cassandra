/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ActorSystem
import akka.cassandra.session.javadsl.CassandraSessionRegistry
import akka.cassandra.session.{ javadsl, CassandraSessionSettings }
import akka.event.Logging
import akka.stream.alpakka.cassandra.scaladsl.CassandraSpecBase
import akka.stream.javadsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

final class CassandraSessionSpec extends CassandraSpecBase(ActorSystem("CassandraSessionSpec")) {

  val log = Logging(system, this.getClass)
  val javadslSessionRegistry = CassandraSessionRegistry.get(system)

  val data = 1 until 103

  private val dataTableName = "testcounts"
  lazy val dataTable = s"$keyspaceName.$dataTableName"

  def insertDataTable() = {
    cqlSession.execute(s"""CREATE TABLE IF NOT EXISTS $dataTable (
                          |  partition text,
                          |  key text,
                          |  count bigint,
                          |  PRIMARY KEY (partition, key)
                          |)
                          |""".stripMargin)
    executeCql(
      immutable.Seq(
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('A', 'a', 1);",
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('A', 'b', 2);",
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('A', 'c', 3);",
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('A', 'd', 4);",
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('B', 'e', 5);",
        s"INSERT INTO $dataTable (partition, key, count) VALUES ('B', 'f', 6);"))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    insertDataTable()
  }

  val sessionSettings: CassandraSessionSettings = CassandraSessionSettings("alpakka.cassandra")

  // testing javadsl to prove delegation works
  lazy val session: javadsl.CassandraSession = javadslSessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  def await[T](cs: CompletionStage[T]): T = cs.toScala.futureValue

  "session" must {

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped {
      val session = javadslSessionRegistry.sessionFor(sessionSettings, system.dispatcher)
      val table = createTableName()
      await(session.executeDDL(s"""
             |CREATE TABLE IF NOT EXISTS $table (
             |    id int PRIMARY KEY
             |);""".stripMargin))
      Future
        .sequence(data.map { i =>
          session.executeWrite(s"INSERT INTO $table(id) VALUES ($i)").toScala
        })
        .map(_ => Done)
        .futureValue mustBe Done

      val rows = session.select(s"SELECT * FROM $table").runWith(Sink.seq, materializer).toScala.futureValue
      rows.asScala.map(_.getInt("id")) must contain theSameElementsAs data
    }

  }

  "CassandraSession" must {

    "select prepared Statement[_]as Source" in {
      val stmt = await(session.prepare(s"SELECT count FROM $dataTable WHERE partition = ?"))
      val bound = stmt.bind("A")
      val rows = session.select(bound).asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10).expectNextUnordered(1L, 2L, 3L, 4L).expectComplete()
      }
    }

    "select and bind as Source" in {
      val rows = session.select(s"SELECT count FROM $dataTable WHERE partition = ?", "B").asScala
      val probe = rows.map(_.getLong("count")).runWith(TestSink.probe[Long])
      probe.within(10.seconds) {
        probe.request(10).expectNextUnordered(5L, 6L).expectComplete()
      }
    }
    "selectAll and bind" in {
      val rows = await(session.selectAll(s"SELECT count FROM $dataTable WHERE partition = ?", "A"))
      rows.asScala.map(_.getLong("count")).toSet mustBe Set(1L, 2L, 3L, 4L)
    }

    "selectAll empty" in {
      val rows = await(session.selectAll(s"SELECT count FROM $dataTable WHERE partition = ?", "X"))
      rows mustBe empty
    }

    "selectOne and bind" in {
      val row = await(session.selectOne(s"SELECT count FROM $dataTable WHERE partition = ? and key = ?", "A", "b"))
      row.get.getLong("count") mustBe 2L
    }

    "selectOne empty" in {
      val row = await(session.selectOne(s"SELECT count FROM $dataTable WHERE partition = ? and key = ?", "A", "x"))
      row mustBe empty
    }

    "create indexes" in {
      await(session.executeDDL(s"CREATE INDEX IF NOT EXISTS count_idx ON $dataTable(count)"))
      val row =
        await(
          session.selectOne("SELECT * FROM system_schema.indexes WHERE table_name = ? ALLOW FILTERING", dataTableName))
      row.asScala.map(index => index.getString("table_name") -> index.getString("index_name")) mustBe Some(
        dataTableName -> "count_idx")
    }

  }
}
