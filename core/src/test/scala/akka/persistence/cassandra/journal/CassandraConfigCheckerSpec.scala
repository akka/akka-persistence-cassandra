/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.persistence._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraPluginConfig }
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.Try

object CassandraConfigCheckerSpec {
  val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |cassandra-journal.target-partition-size = 5
      |cassandra-journal.max-result-size = 3
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

  class DummyActor(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = {
      case x => ()
    }

    def receiveCommand: Receive = {
      case x: String => persist(x) { msg => receiver ! s"Received $msg" }
    }
  }
}

import akka.persistence.cassandra.journal.CassandraConfigCheckerSpec._

class CassandraConfigCheckerSpec extends TestKit(ActorSystem("CassandraConfigCheckerSpec", config))
  with ImplicitSender with WordSpecLike with MustMatchers with CassandraLifecycle {

  import system.dispatcher
  implicit val cfg = config.withFallback(system.settings.config).getConfig("cassandra-journal")
  implicit val pluginConfig = new CassandraPluginConfig(system, cfg)

  override def systemName: String = "CassandraConfigCheckerSpec"

  lazy val session = {
    Await.result(pluginConfig.sessionProvider.connect(), 5.seconds)
  }

  override protected def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
    super.afterAll()
  }

  "CassandraConfigChecker" should {

    "persist value in cassandra" in {
      waitForPersistenceInitialization()
      val underTest = createCassandraConfigChecker
      session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")

      val persistentConfig = Await.result(underTest.initializePersistentConfig(session), remainingOrDefault)
      persistentConfig.get(CassandraJournalConfig.TargetPartitionProperty) must be(defined)
      persistentConfig(CassandraJournalConfig.TargetPartitionProperty) must be("5")
      getTargetSize(underTest) must be("5")
    }

    "multiple persistence should keep the same value" in {
      waitForPersistenceInitialization()
      session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")

      (1 to 5).foreach(_ => {
        val underTest = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef("5")))
        val persistentConfig = Await.result(underTest.initializePersistentConfig(session), remainingOrDefault)
        persistentConfig(CassandraJournalConfig.TargetPartitionProperty) must be("5")
        assert(persistentConfig.contains(CassandraJournalConfig.TargetPartitionProperty))
        getTargetSize(underTest) must be("5")
      })
    }

    "throw exception when starting with wrong value" in {
      waitForPersistenceInitialization()
      val underTest = createCassandraConfigChecker
      session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")
      Await.result(underTest.initializePersistentConfig(session), remainingOrDefault)

      val try3Size = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef("3")))
      intercept[IllegalArgumentException] {
        Await.result(try3Size.initializePersistentConfig(session), remainingOrDefault)
      }

      getTargetSize(underTest) must be("5")
    }

    "concurrent calls keep consistent value" in {
      waitForPersistenceInitialization()
      session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")

      val resultFuture = Future.sequence((1 to 10).map(i => Future {
        (i, Try {
          val underTest = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef(i.toString)))
          Await.result(underTest.initializePersistentConfig(session), remainingOrDefault)
        })
      }))

      val result = Await.result(resultFuture, 5.seconds)

      val firstSize = getTargetSize(createCassandraConfigChecker)

      val (success, failure) = result.partition(_._1 == firstSize.toInt)
      success.size must be(1)
      success.head._2.isSuccess must be(true)
      success.head._2.get(CassandraJournalConfig.TargetPartitionProperty) must be(firstSize)

      failure.foreach(f =>
        intercept[IllegalArgumentException] {
          f._2.get
        })
    }
  }

  def createCassandraConfigChecker(implicit pluginConfig: CassandraPluginConfig, cfg: Config): CassandraStatements =
    new CassandraStatements {
      override def config: CassandraJournalConfig = new CassandraJournalConfig(system, cfg)
    }

  def waitForPersistenceInitialization(): Unit = {
    val actor = system.actorOf(Props(classOf[DummyActor], "p1", self))
    actor ! "Hi"
    expectMsg("Received Hi")
    actor ! PoisonPill
  }

  def getTargetSize(checker: CassandraStatements): String = {
    session.execute(s"${checker.selectConfig} WHERE property='${CassandraJournalConfig.TargetPartitionProperty}'").one().getString("value")
  }
}
