/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraFlow, CassandraSession, CassandraSpecBase }
import akka.stream.alpakka.cassandra.{ CassandraSessionSettings, CassandraWriteSettings }
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped

import scala.concurrent.duration._

final class CassandraSessionPerformanceSpec extends CassandraSpecBase(ActorSystem("CassandraSessionPerformanceSpec")) {

  val log = Logging(system, this.getClass)

  val data = 1 until 1000 * 500

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(2.minutes, 100.millis)

  private val dataTableName = "largerData"
  lazy val dataTable = s"$keyspaceName.$dataTableName"

  val sessionSettings: CassandraSessionSettings = CassandraSessionSettings()
  override val lifecycleSession: CassandraSession =
    sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  lazy val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  def insertDataTable() = {
    lifecycleSession
      .executeDDL(s"""CREATE TABLE IF NOT EXISTS $dataTable (
                     |    id int,
                     |    value int,
                     |    PRIMARY KEY (id, value)
                     |);""".stripMargin)
      .flatMap { _ =>
        Source(data)
          .via {
            CassandraFlow.createUnloggedBatch(
              CassandraWriteSettings.create().withMaxBatchSize(10000),
              s"INSERT INTO $dataTable(id, value) VALUES (?, ?)",
              (d: Int, ps) => ps.bind(Int.box(d), Int.box(d % 100)),
              (d: Int) => d % 100)(lifecycleSession)
          }
          .runWith(Sink.ignore)
      }
      .futureValue
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    insertDataTable()
  }

  "Select" must {
    "read many rows" in assertAllStagesStopped {
      val rows =
        session
          .select(s"SELECT * FROM $dataTable")
          .map(_.getInt("id"))
          .runWith(Sink.fold(0)((u, _) => u + 1))
          .futureValue
      rows mustBe data.last
    }
  }
}
