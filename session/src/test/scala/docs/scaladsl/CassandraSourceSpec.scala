/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.cassandra.session.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestKit
import com.datastax.oss.driver.api.core.CqlSession
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
    with Matchers {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  //#init-mat
  implicit val mat: Materializer = ActorMaterializer()
  //#init-mat

  def port(): Int = 9042

  lazy val session = {
    CqlSession
      .builder()
      .withLocalDatacenter("datacenter1")
      .addContactPoint(new InetSocketAddress("127.0.0.1", port()))
      .build()
  }

  implicit val ec = system.dispatcher

  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
  val data = 1 until 103

  def withKeyspace(prepare: (String, CqlSession) => Unit, testCode: String => Unit): Unit = {
    val keyspaceName: String = s"akka${System.nanoTime()}"
    session.execute(s"""
         |CREATE KEYSPACE $keyspaceName WITH replication = {
         |  'class': 'SimpleStrategy',
         |  'replication_factor': '1'
         |};
      """.stripMargin)
    prepare(keyspaceName, session)
    testCode(keyspaceName)
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspaceName;")
  }

  override def afterAll(): Unit = {
    session.close()
    shutdown()
  }

  "CassandraSourceSpec" must {

    "stream the result of a Cassandra statement with one page" in assertAllStagesStopped(
      withKeyspace(prepare = (keyspaceName, session) => {
        session.execute(s"""
                         |CREATE TABLE IF NOT EXISTS $keyspaceName.test (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
        data.foreach { i =>
          session.execute(s"INSERT INTO $keyspaceName.test(id) VALUES ($i)")
        }

      }, testCode = keyspaceName => {
        val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
        val rows = CassandraSource(sessionSettings, s"SELECT * FROM $keyspaceName.test").runWith(Sink.seq).futureValue

        rows.map(_.getInt("id")) must contain theSameElementsAs data
      }))

    "support parameters" in assertAllStagesStopped(
      withKeyspace(
        prepare = (keyspaceName, session) => {
          session.execute(s"""
                         |CREATE TABLE IF NOT EXISTS $keyspaceName.test (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
          data.foreach { i =>
            session.execute(s"INSERT INTO $keyspaceName.test(id) VALUES ($i)")
          }

        },
        testCode = keyspaceName => {
          val rows = CassandraSource(sessionSettings, s"SELECT * FROM $keyspaceName.test WHERE id = ?", Int.box(5))
            .runWith(Sink.seq)
            .futureValue

          rows.map(_.getInt("id")) mustBe Seq(5)
        }))

    "stream the result of a Cassandra statement with several pages" in assertAllStagesStopped(
      withKeyspace(prepare = (keyspaceName, session) => {
        session.execute(s"""
                             |CREATE TABLE IF NOT EXISTS $keyspaceName.test (
                             |    id int PRIMARY KEY
                             |);""".stripMargin)
        data.foreach { i =>
          session.execute(s"INSERT INTO $keyspaceName.test(id) VALUES ($i)")
        }

      }, testCode = keyspaceName => {

        //#statement
        val stmt = SimpleStatement.newInstance(s"SELECT * FROM $keyspaceName.test").setPageSize(20)
        //#statement

        //#run-source
        val rows = CassandraSource(sessionSettings, stmt).runWith(Sink.seq)
        //#run-source

        rows.futureValue.map(_.getInt("id")) must contain theSameElementsAs data
      }))

  }
}
