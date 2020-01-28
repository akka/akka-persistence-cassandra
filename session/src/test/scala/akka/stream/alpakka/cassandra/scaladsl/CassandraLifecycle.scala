/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.testkit.TestKitBase
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait CassandraLifecycleBase {
  def cqlSession: CqlSession

  def execute(cqlSession: CqlSession, statements: immutable.Seq[BatchableStatement[_]]): ResultSet = {
    val batch = new BatchStatementBuilder(BatchType.LOGGED)
    statements.foreach { stmt =>
      batch.addStatement(stmt)
    }
    cqlSession.execute(batch.build())
  }

  def executeCql(cqlSession: CqlSession, statements: immutable.Seq[String]): ResultSet = {
    execute(cqlSession, statements.map(stmt => SimpleStatement.newInstance(stmt)))
  }

  def createKeyspace(cqlSession: CqlSession, name: String): Unit =
    cqlSession.execute(
      s"""CREATE KEYSPACE $name WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'};""")

  def dropKeyspace(cqlSession: CqlSession, name: String): Unit =
    cqlSession.execute(s"""DROP KEYSPACE IF EXISTS $name;""")

  def createKeyspace(name: String): Unit = createKeyspace(cqlSession, name)

  def dropKeyspace(name: String): Unit = dropKeyspace(cqlSession, name)

  def execute(statements: immutable.Seq[BatchableStatement[_]]): ResultSet = execute(cqlSession, statements)

  def executeCql(statements: immutable.Seq[String]): ResultSet = executeCql(cqlSession, statements)

  def executeCqlList(statements: java.util.List[String]): ResultSet = executeCql(cqlSession, statements.asScala.toList)

}

trait CassandraLifecycle extends BeforeAndAfterAll with TestKitBase with CassandraLifecycleBase {
  this: Suite =>

  def port(): Int = 9042

  lazy val cqlSession = {
    CqlSession
      .builder()
      .withLocalDatacenter("datacenter1")
      .addContactPoint(new InetSocketAddress("127.0.0.1", port()))
      .build()
  }

  def keyspaceNamePrefix: String = getClass.getSimpleName
  final lazy val keyspaceName: String = s"$keyspaceNamePrefix${System.nanoTime()}"

  private val tableNumber = new AtomicInteger()

  def createTableName() = s"$keyspaceName.test${tableNumber.incrementAndGet()}"

  override protected def beforeAll(): Unit = {
    createKeyspace(keyspaceName)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    dropKeyspace(keyspaceName)
    try {
      Await.result(cqlSession.closeAsync().toScala, 20.seconds)
    } catch {
      case NonFatal(e) =>
        e.printStackTrace(System.err)
    }
    super.afterAll()
  }

}

class CassandraAccess(val cqlSession: CqlSession) extends CassandraLifecycleBase
