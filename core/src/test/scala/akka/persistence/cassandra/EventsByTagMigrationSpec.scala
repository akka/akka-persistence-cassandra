/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.nio.ByteBuffer
import java.time.{ LocalDateTime, ZoneOffset }

import akka.{ Done, NotUsed }
import akka.actor.{ ActorSystem, PoisonPill }
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.TestTaggingActor.Ack
import akka.persistence.cassandra.journal.{ CassandraJournalConfig, CassandraStatements, Hour, TimeBucket }
import akka.persistence.cassandra.query.DirectWriting
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ ImplicitSender, TestKit }
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.duration._
import scala.util.Try

/**
 */
object EventsByTagMigrationSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val keyspaceName = "EventsByTagMigration"
  val messagesTableName = s"$keyspaceName.messages"
  val eventsByTagViewName = s"$keyspaceName.eventsByTag1"

  val oldMessagesTable =
    s"""
       | CREATE TABLE $messagesTableName(
       | used boolean static,
       | persistence_id text,
       | partition_nr bigint,
       | sequence_nr bigint,
       | timestamp timeuuid,
       | timebucket text,
       | writer_uuid text,
       | ser_id int,
       | ser_manifest text,
       | event_manifest text,
       | event blob,
       | meta_ser_id int,
       | meta_ser_manifest text,
       | meta blob,
       | tag1 text,
       | tag2 text,
       | tag3 text,
       | message blob,
       | PRIMARY KEY((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
    """.stripMargin

  val oldMaterlializedView =
    s"""
      CREATE MATERIALIZED VIEW $eventsByTagViewName AS
         SELECT tag1, timebucket, timestamp, persistence_id, partition_nr, sequence_nr, writer_uuid, ser_id, ser_manifest, event_manifest, event,
           meta_ser_id, meta_ser_manifest, meta, message
         FROM $messagesTableName
         WHERE persistence_id IS NOT NULL AND partition_nr IS NOT NULL AND sequence_nr IS NOT NULL
           AND tag1 IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
         PRIMARY KEY ((tag1, timebucket), timestamp, persistence_id, partition_nr, sequence_nr)
         WITH CLUSTERING ORDER BY (timestamp ASC)
      """

  val createKeyspace =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $keyspaceName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }
     """.stripMargin

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       | actor.serialize-messages=off
       | loglevel = DEBUG
       | actor.debug.unhandled = on
       |}
       |cassandra-journal {
       | keyspace = $keyspaceName
       | keyspace-autocreate = true
       | tables-autocreate = true
       |}
       |cassandra-query-journal {
       | first-time-bucket = "${today.minusHours(5).format(query.firstBucketFormat)}"
       |}
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

}

class EventsByTagMigrationSpec extends TestKit(ActorSystem(EventsByTagMigrationSpec.keyspaceName, EventsByTagMigrationSpec.config))
  with WordSpecLike
  with CassandraLifecycle
  with DirectWriting
  with ScalaFutures
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender {

  import EventsByTagMigrationSpec._

  val statements = new CassandraStatements {
    override def config: CassandraJournalConfig = new CassandraJournalConfig(system, EventsByTagMigrationSpec.config)
  }

  lazy val session = cluster.connect()
  override val systemName = keyspaceName
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(500, Milliseconds))
  implicit val materialiser = ActorMaterializer()(system)
  val waitTime = 100.millis
  lazy val migrator = EventsByTagMigration(system)
  lazy val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // Lazy so they don't get created until the schema changes have happened
  lazy val systemTwo = ActorSystem("EventsByTagMigration-2", config)
  lazy val materialiserTwo = ActorMaterializer()(systemTwo)
  lazy val queriesTwo = PersistenceQuery(systemTwo).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // for after tag1-3 columns are dropped
  lazy val systemThree = ActorSystem("EventsByTagMigration-3", config)
  lazy val materialiserThree = ActorMaterializer()(systemThree)
  lazy val queriesThree = PersistenceQuery(systemThree).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    system.log.debug("Creating old tables")
    // Drop the messages table as we want to start with the old one
    session.execute(s"drop table ${messagesTableName}")
    session.execute(oldMessagesTable)
    session.execute(oldMaterlializedView)
    system.log.debug("Old tables created")
  }

  "Events by tag migration with no metadata" must {
    val pidOne = "p-1"
    val pidTwo = "p-2"

    "have some existing tagged messages" in {
      writeOldTestEventWithTags(PersistentRepr("e-1", 1L, pidOne), Set("blue", "green", "orange"))
      writeOldTestEventWithTags(PersistentRepr("e-2", 2L, pidOne), Set("blue"))
      writeOldTestEventWithTags(PersistentRepr("e-3", 3L, pidOne), Set())
      writeOldTestEventWithTags(PersistentRepr("e-4", 4L, pidOne), Set("blue", "green"))
      writeOldTestEventWithTags(PersistentRepr("f-1", 1L, pidTwo), Set("green"))
      writeOldTestEventWithTags(PersistentRepr("f-2", 2L, pidTwo), Set("blue"))
    }

    "allow creation of the new tags view table" in {
      migrator.createTables().futureValue shouldEqual Done
    }

    "migrate tags to the new table" in {
      migrator.migrateToTagViews().futureValue shouldEqual Done
    }

    "allow adding of the new tags column" in {
      migrator.addTagsColumn()
    }

    "work with the current implementation" in {
      val blueSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])
      blueProbe.request(5)
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 4, "e-4") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 2, "f-2") => }
      blueProbe.expectNoMessage(waitTime)
      blueProbe.cancel()

      val greenSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("green", NoOffset)
      val greenProbe = greenSrc.runWith(TestSink.probe[Any])
      greenProbe.request(4)
      greenProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      greenProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 4, "e-4") => }
      greenProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 1, "f-1") => }
      greenProbe.expectNoMessage(waitTime)
      greenProbe.cancel()

      val orangeSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("orange", NoOffset)
      val orangeProbe = orangeSrc.runWith(TestSink.probe[Any])
      orangeProbe.request(3)
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      orangeProbe.expectNoMessage(waitTime)
      orangeProbe.cancel()

      val bananaSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("banana", NoOffset)
      val bananaProbe = bananaSrc.runWith(TestSink.probe[Any])
      bananaProbe.request(3)
      bananaProbe.expectNoMessage(waitTime)
      bananaProbe.cancel()
    }
    // This will be left as a manual step for the user as it stops
    // rolling back to the old version
    "allow dropping of the materialized view" in {
      session.execute(s"DROP MATERIALIZED VIEW $eventsByTagViewName")
    }

    "have a peek in the messages table" in {
      val row = session.execute(s"select * from ${messagesTableName} limit 1").one()
      system.log.debug("New messages table looks like: {}", row)
      system.log.debug("{}", row.getColumnDefinitions)
    }

    "be able to add tags to existing pids" in {
      // we need a new actor system for this as the old one will have prepared the statements without
      // the tags column existing
      val pidOnePA = systemTwo.actorOf(TestTaggingActor.props(pidOne, Set("blue", "yellow")))
      pidOnePA ! "new-event-1"
      expectMsg(Ack)
      pidOnePA ! "new-event-2"
      expectMsg(Ack)

      val blueSrc: Source[EventEnvelope, NotUsed] = queriesTwo.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])(materialiserTwo)
      blueProbe.request(7)
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 4, "e-4") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 2, "f-2") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 5, "new-event-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 6, "new-event-2") => }
      blueProbe.expectNoMessage(waitTime)
      blueProbe.cancel()
      pidOnePA ! PoisonPill
    }

    // Again a manual step, leaving them is only wasting disk space
    // the new version will work with these columns still there
    "allow dropping of tag columns" in {
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag1")
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag2")
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag3")
    }

    "still work after dropping the tag columns" in {
      val pidTwoPA = systemThree.actorOf(TestTaggingActor.props(pidTwo, Set("orange")))
      pidTwoPA ! "new-event-1"
      expectMsg(Ack)
      pidTwoPA ! "new-event-2"
      expectMsg(Ack)

      val orangeSrc: Source[EventEnvelope, NotUsed] = queriesThree.eventsByTag("orange", NoOffset)
      val orangeProbe = orangeSrc.runWith(TestSink.probe[Any])(materialiserThree)
      orangeProbe.request(3)
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 3, "new-event-1") => }
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 4, "new-event-2") => }
      orangeProbe.expectNoMessage(waitTime)
      orangeProbe.expectNoMessage(waitTime)
      orangeProbe.cancel()
      pidTwoPA ! PoisonPill
    }
  }

  "Events by tag migration with messages in the message column" must {
    "work" in {
      pending
    }
  }

  "Events by tag migration metadata" must {
    "work" in {
      pending
    }
  }

  "Events by tag resuming migration" must {
    "work" in {
      pending
    }
  }

  "Events by tag partial migration with recovery fixing the rest" must {
    "work" in {
      pending
    }
  }

  private lazy val serialization = SerializationExtension(system)

  override protected def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
    super.afterAll()
    shutdown(systemTwo)
    shutdown(systemThree)
  }

  // Write used before 0.80
  val writeMessage =
    s"""
      INSERT INTO ${messagesTableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event,
        tag1, tag2, tag3, used)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true)
    """

  private lazy val preparedWriteMessage = {
    session.prepare(writeMessage)
  }

  protected def writeOldTestEventWithTags(persistent: PersistentRepr, tags: Set[String]): Unit = {
    require(tags.size <= 3)
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)

    val serManifest = serializer match {
      case ser2: SerializerWithStringManifest ⇒
        ser2.manifest(persistent)
      case _ ⇒
        if (serializer.includeManifest) persistent.getClass.getName
        else PersistentRepr.Undefined
    }

    val bs = preparedWriteMessage.bind()
    tags.zipWithIndex.foreach {
      case (tag, index) =>
        bs.setString(s"tag${index + 1}", tag)
    }

    bs.setString("persistence_id", persistent.persistenceId)
    bs.setLong("partition_nr", 0L)
    bs.setLong("sequence_nr", persistent.sequenceNr)
    val nowUuid = UUIDs.timeBased()
    val now = UUIDs.unixTimestamp(nowUuid)
    bs.setUUID("timestamp", nowUuid)
    bs.setString("timebucket", TimeBucket(now, Hour).key.toString)
    bs.setInt("ser_id", serializer.identifier)
    bs.setString("ser_manifest", serManifest)
    bs.setString("event_manifest", persistent.manifest)
    bs.setBytes("event", serialized)
    session.execute(bs)
    system.log.debug("Directly wrote payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

  override protected def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .addContactPoint("localhost")
      .withClusterName(systemName + "Cleanup")
      .build()
    Try(cluster.connect().execute(s"drop keyspace $keyspaceName"))
    cluster.close()
  }
}
