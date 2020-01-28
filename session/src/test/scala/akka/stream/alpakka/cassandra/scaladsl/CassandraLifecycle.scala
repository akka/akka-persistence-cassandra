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

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.compat.java8.FutureConverters._

trait CassandraLifecycle extends BeforeAndAfterAll with TestKitBase {
  this: Suite =>

  def systemName: String = getClass.getSimpleName

  def port(): Int = 9042

  lazy val cqlSession = {
    CqlSession
      .builder()
      .withLocalDatacenter("datacenter1")
      .addContactPoint(new InetSocketAddress("localhost", port()))
      .build()
  }

  final val keyspaceName: String = s"$systemName${System.nanoTime()}"

  private val tableNumber = new AtomicInteger()

  def createTableName() = s"$keyspaceName.test${tableNumber.incrementAndGet()}"

  override protected def beforeAll(): Unit = {
    createKeyspace(keyspaceName)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    shutdown(system, verifySystemShutdown = true)
    dropKeyspace(keyspaceName)
    Await.result(cqlSession.closeAsync().toScala, 10.seconds)
    super.afterAll()
  }

  def createKeyspace(name: String): Unit =
    cqlSession.execute(
      s"""CREATE KEYSPACE $name WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'};""")

  def dropKeyspace(name: String): Unit =
    cqlSession.execute(s"""DROP KEYSPACE IF EXISTS $name;""")

  def execute(statements: immutable.Seq[BatchableStatement[_]]): ResultSet = {
    val batch = new BatchStatementBuilder(BatchType.LOGGED)
    statements.foreach { stmt =>
      batch.addStatement(stmt)
    }
    cqlSession.execute(batch.build())
  }

  def executeCql(statements: immutable.Seq[String]): ResultSet = {
    execute(statements.map(stmt => SimpleStatement.newInstance(stmt)))
  }

}
