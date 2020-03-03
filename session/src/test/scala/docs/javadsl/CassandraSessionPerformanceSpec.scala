/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSpecBase }
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BatchType }

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

final class CassandraSessionPerformanceSpec extends CassandraSpecBase(ActorSystem("CassandraSessionPerformanceSpec")) {

  val log = Logging(system, this.getClass)

  val data = 1 until 1000 * 100

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1.minute)

  private val dataTableName = "largerData"
  lazy val dataTable = s"$keyspaceName.$dataTableName"

  val sessionSettings: CassandraSessionSettings = CassandraSessionSettings()
  override val lifecycleSession: CassandraSession =
    sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  lazy val session: CassandraSession = sessionRegistry.sessionFor(sessionSettings, system.dispatcher)

  def insertDataTable() = {
    lifecycleSession
      .executeDDL(s"""CREATE TABLE IF NOT EXISTS $dataTable (
                     |    id int PRIMARY KEY
                     |);""".stripMargin)
      .flatMap { _ =>
        lifecycleSession.prepare(s"INSERT INTO $dataTable(id) VALUES (?)")
      }
      .map { prepareStatement =>
        data.sliding(10000, 10000).foreach { d =>
          val boundStatements = d.map { i =>
            prepareStatement.bind(Int.box(i))
          }
          val batchStatement = BatchStatement.newInstance(BatchType.LOGGED).addAll(boundStatements.asJava)
          Await.result(lifecycleSession.executeWrite(batchStatement), 4.seconds)
        }
        Done
      }
      .futureValue
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    insertDataTable()
  }

  "session" must {
    "stream the result of a Cassandra statement with one page" ignore assertAllStagesStopped {
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
