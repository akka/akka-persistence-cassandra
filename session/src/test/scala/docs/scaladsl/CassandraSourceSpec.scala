/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSource, CassandraSpecBase }
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.datastax.oss.driver.api.core.cql.SimpleStatement

class CassandraSourceSpec extends CassandraSpecBase(ActorSystem("CassandraSourceSpec")) {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
  val data = 1 until 103

  override val lifecycleSession: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  "CassandraSourceSpec" must {
    implicit val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped {

      val table = createTableName()
      lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY
                         |);""".stripMargin).futureValue mustBe Done
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)")).futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table").runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "support parameters" in assertAllStagesStopped {
      val table = createTableName()
      lifecycleSession.executeDDL(s"""
                                   |CREATE TABLE IF NOT EXISTS $table (
                                   |    id int PRIMARY KEY
                                   |);""".stripMargin).futureValue mustBe Done
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)")).futureValue mustBe Done

      val rows =
        CassandraSource(s"SELECT * FROM $table WHERE id = ?", Int.box(5)).runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) mustBe Seq(5)
    }

    "stream the result of a Cassandra statement with several pages" in assertAllStagesStopped {
      val table = createTableName()
      lifecycleSession.executeDDL(s"""
                                   |CREATE TABLE IF NOT EXISTS $table (
                                   |    id int PRIMARY KEY
                                   |);""".stripMargin).futureValue mustBe Done
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)")).futureValue mustBe Done

      //#statement
      val stmt = SimpleStatement.newInstance(s"SELECT * FROM $table").setPageSize(20)
      //#statement

      //#run-source
      val rows = CassandraSource(stmt).runWith(Sink.seq)
      //#run-source

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "allow prepared statements" in assertAllStagesStopped {
      val table = createTableName()
      lifecycleSession.executeDDL(s"""
                                   |CREATE TABLE IF NOT EXISTS $table (
                                   |    id int PRIMARY KEY
                                   |);""".stripMargin).futureValue mustBe Done
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)")).futureValue mustBe Done

      val stmt = session.prepare(s"SELECT * FROM $table").map(_.bind())
      val rows = CassandraSource.fromFuture(stmt).runWith(Sink.seq)

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

  }
}
