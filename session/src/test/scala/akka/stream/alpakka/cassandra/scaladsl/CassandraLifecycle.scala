/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.testkit.TestKitBase
import com.datastax.oss.driver.api.core.cql._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NonFatal
import scala.compat.java8.FutureConverters._

trait CassandraLifecycleBase {
  def lifecycleSession: CassandraSession

  def execute(session: CassandraSession, statements: immutable.Seq[BatchableStatement[_]]): Future[Done] = {
    val batch = new BatchStatementBuilder(BatchType.LOGGED)
    statements.foreach { stmt =>
      batch.addStatement(stmt)
    }
    session.executeWriteBatch(batch.build())
  }

  def executeCql(session: CassandraSession, statements: immutable.Seq[String]): Future[Done] = {
    execute(session, statements.map(stmt => SimpleStatement.newInstance(stmt)))
  }

  def createKeyspace(session: CassandraSession, name: String): Future[Done] =
    session.executeDDL(
      s"""CREATE KEYSPACE $name WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'};""")

  def dropKeyspace(session: CassandraSession, name: String): Future[Done] =
    session.executeDDL(s"""DROP KEYSPACE IF EXISTS $name;""")

  def createKeyspace(name: String): Future[Done] = createKeyspace(lifecycleSession, name)

  def dropKeyspace(name: String): Future[Done] = dropKeyspace(lifecycleSession, name)

  def execute(statements: immutable.Seq[BatchableStatement[_]]): Future[Done] = execute(lifecycleSession, statements)

  def executeCql(statements: immutable.Seq[String]): Future[Done] = executeCql(lifecycleSession, statements)

  def executeCqlList(statements: java.util.List[String]): CompletionStage[Done] =
    executeCql(lifecycleSession, statements.asScala.toList).toJava

}

trait CassandraLifecycle extends BeforeAndAfterAll with TestKitBase with CassandraLifecycleBase with ScalaFutures {
  this: Suite =>

  def port(): Int = 9042

  def lifecycleSession: CassandraSession

  def keyspaceNamePrefix: String = getClass.getSimpleName
  final lazy val keyspaceName: String = s"$keyspaceNamePrefix${System.nanoTime()}"

  private val tableNumber = new AtomicInteger()

  def createTableName() = s"$keyspaceName.test${tableNumber.incrementAndGet()}"

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  override protected def beforeAll(): Unit = {
    createKeyspace(keyspaceName).futureValue
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    // `dropKeyspace` uses the system dispatcher through `cassandraSession`
    dropKeyspace(keyspaceName).futureValue
    shutdown(system, verifySystemShutdown = true)
    try {
      Await.result(lifecycleSession.close(scala.concurrent.ExecutionContext.global), 20.seconds)
    } catch {
      case NonFatal(e) =>
        e.printStackTrace(System.err)
    }
    super.afterAll()
  }

}

class CassandraAccess(val lifecycleSession: CassandraSession) extends CassandraLifecycleBase
