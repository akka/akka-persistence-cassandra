/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.persistence.cassandra.testkit.CassandraLauncher
import java.util.concurrent.Executors

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.persistence._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraPluginConfig }
import akka.testkit.{ ImplicitSender, TestKit }
import com.datastax.driver.core.Session
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object CassandraConfigCheckerSpec {
  val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |cassandra-journal.target-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
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

class CassandraConfigCheckerSpec extends TestKit(ActorSystem("CassandraConfigCheckerSpec", config)) with ImplicitSender with WordSpecLike with MustMatchers with CassandraLifecycle {

  implicit val cfg = config.withFallback(system.settings.config).getConfig("cassandra-journal")
  implicit val pluginConfig = new CassandraPluginConfig(system, cfg)

  override def systemName: String = "CassandraConfigCheckerSpec"

  "CassandraConfigChecker" should {

    "persist value in cassandra" in {
      waitForPersistenceInitialization()
      val underTest = createCassandraConfigChecker
      underTest.session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")

      val persistentConfig = underTest.initializePersistentConfig
      persistentConfig.get(CassandraJournalConfig.TargetPartitionProperty) must be(defined)
      persistentConfig.get(CassandraJournalConfig.TargetPartitionProperty).get must be("5")
      getTargetSize(underTest) must be("5")
    }

    "multiple persistence should keep the same value" in {
      waitForPersistenceInitialization()
      val underTest = createCassandraConfigChecker
      underTest.session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")

      (1 to 5).foreach(i => {
        val underTest = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef("5")))
        val persistentConfig = underTest.initializePersistentConfig
        persistentConfig.get(CassandraJournalConfig.TargetPartitionProperty).get must be("5")
        assert(persistentConfig.contains(CassandraJournalConfig.TargetPartitionProperty))
        getTargetSize(underTest) must be("5")
      })
    }

    "throw exception when starting with wrong value" in {
      waitForPersistenceInitialization()
      val underTest = createCassandraConfigChecker
      underTest.session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")
      underTest.initializePersistentConfig

      val try3Size = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef("3")))
      intercept[IllegalArgumentException] {
        try3Size.initializePersistentConfig
      }

      getTargetSize(underTest) must be("5")
    }

    "concurrent calls keep consistent value" in {
      waitForPersistenceInitialization()
      createCassandraConfigChecker.session.execute(s"TRUNCATE ${pluginConfig.keyspace}.${pluginConfig.configTable}")
      implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

      val resultFuture = Future.sequence((1 to 10).map(i => Future {
        (i, Try {
          val underTest = createCassandraConfigChecker(pluginConfig, cfg.withValue("target-partition-size", ConfigValueFactory.fromAnyRef(i.toString)))
          underTest.initializePersistentConfig
        })
      }))

      val result = Await.result(resultFuture, 5.seconds)

      val firstSize = getTargetSize(createCassandraConfigChecker)

      val (success, failure) = result.partition(_._1 == firstSize.toInt)
      success.size must be(1)
      success.head._2.isSuccess must be(true)
      success.head._2.get.get(CassandraJournalConfig.TargetPartitionProperty).get must be(firstSize)

      failure.foreach(f =>
        intercept[IllegalArgumentException] {
          f._2.get
        })
    }
  }

  def createCassandraConfigChecker(implicit pluginConfig: CassandraPluginConfig, cfg: Config): CassandraConfigChecker = {

    val clusterSession = {
      import system.dispatcher
      Await.result(pluginConfig.sessionProvider.connect(), 5.seconds)
    }

    new CassandraConfigChecker {
      override def session: Session = clusterSession
      override def config: CassandraJournalConfig = new CassandraJournalConfig(system, cfg)
    }
  }

  def waitForPersistenceInitialization() = {
    val actor = system.actorOf(Props(classOf[DummyActor], "p1", self))
    actor ! "Hi"
    expectMsg("Received Hi")
    actor ! PoisonPill
  }

  def getTargetSize(checker: CassandraConfigChecker): String = {
    checker.session.execute(s"${checker.selectConfig} WHERE property='${CassandraJournalConfig.TargetPartitionProperty}'").one().getString("value")
  }
}
