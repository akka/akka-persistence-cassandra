/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.cassandra.session.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraLifecycle, CassandraSource }
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestKit
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
class CassandraSourceSpec
    extends TestKit(ActorSystem("CassandraSourceSpec"))
    with AnyWordSpecLike
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with CassandraLifecycle {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  //#init-mat
  implicit val mat: Materializer = ActorMaterializer()
  //#init-mat

  implicit val ec = system.dispatcher

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
  val data = 1 until 103

  "CassandraSourceSpec" must {

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped {
      val table = createTableName()
      cqlSession.execute(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)"))

      val rows = CassandraSource(sessionSettings, s"SELECT * FROM $table").runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "support parameters" in assertAllStagesStopped {
      val table = createTableName()
      cqlSession.execute(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)"))

      val rows =
        CassandraSource(sessionSettings, s"SELECT * FROM $table WHERE id = ?", Int.box(5)).runWith(Sink.seq).futureValue

      rows.map(_.getInt("id")) mustBe Seq(5)
    }

    "stream the result of a Cassandra statement with several pages" in assertAllStagesStopped {
      val table = createTableName()
      cqlSession.execute(s"""
                             |CREATE TABLE IF NOT EXISTS $table (
                             |    id int PRIMARY KEY
                             |);""".stripMargin)
      executeCql(data.map(i => s"INSERT INTO $table(id) VALUES ($i)"))

      //#statement
      val stmt = SimpleStatement.newInstance(s"SELECT * FROM $table").setPageSize(20)
      //#statement

      //#run-source
      val rows = CassandraSource(sessionSettings, stmt).runWith(Sink.seq)
      //#run-source

      rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
    }

  }
}
