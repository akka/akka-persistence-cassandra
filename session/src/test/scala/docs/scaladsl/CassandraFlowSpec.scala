/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.{ CassandraSessionSettings, CassandraWriteSettings }
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraFlow, CassandraSession, CassandraSource, CassandraSpecBase }
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.collection.immutable

class CassandraFlowSpec extends CassandraSpecBase(ActorSystem("CassandraFlowSpec")) {

  //#element-to-insert
  case class ToInsert(id: Integer, cc: Integer)
  //#element-to-insert

  val sessionSettings = CassandraSessionSettings("alpakka.cassandra")
  val data = 1 until 103

  override val lifecycleSession: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  "CassandraFlow" must {
    implicit val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

    "update with simple prepared statement" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY
                         |);""".stripMargin)
      }.futureValue mustBe Done

      val written = Source(data)
        .via(
          CassandraFlow.create(
            session,
            CassandraWriteSettings.defaults,
            s"INSERT INTO $table(id) VALUES (?)",
            (element, preparedStatement) => preparedStatement.bind(Int.box(element))))
        .runWith(Sink.ignore)
      written.futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table").runWith(Sink.seq).futureValue
      rows.map(_.getInt("id")) must contain theSameElementsAs data
    }

    "update with prepared statement" in assertAllStagesStopped {
      val table = createTableName()
      withSchemaMetadataDisabled {
        lifecycleSession.executeDDL(s"""
                         |CREATE TABLE IF NOT EXISTS $table (
                         |    id int PRIMARY KEY,
                         |    name text,
                         |    city text
                         |);""".stripMargin)
      }.futureValue mustBe Done

      case class Person(id: Int, name: String, city: String)

      val persons =
        immutable.Seq(Person(12, "John", "London"), Person(43, "Umberto", "Roma"), Person(56, "James", "Chicago"))
      val written = Source(persons)
        .via(CassandraFlow.create(
          session,
          CassandraWriteSettings.defaults,
          s"INSERT INTO $table(id, name, city) VALUES (?, ?, ?)",
          (person, preparedStatement) => preparedStatement.bind(Int.box(person.id), person.name, person.city)))
        .runWith(Sink.ignore)
      written.futureValue mustBe Done

      val rows = CassandraSource(s"SELECT * FROM $table")
        .map { row =>
          Person(row.getInt("id"), row.getString("name"), row.getString("city"))
        }
        .runWith(Sink.seq)
        .futureValue
      rows must contain theSameElementsAs persons
    }
  }
}
