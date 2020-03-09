/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.nio.ByteBuffer
import java.lang.{ Long => JLong }

import akka.actor.{ ActorSystem, PoisonPill }
import akka.persistence.cassandra.TestTaggingActor.Ack
import akka.persistence.cassandra.journal.CassandraJournalStatements
import akka.persistence.cassandra.query.DirectWriting
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.persistence.{ PersistentRepr, RecoveryCompleted }
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import akka.{ Done, NotUsed }
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

import akka.persistence.cassandra.journal.TimeBucket
import akka.serialization.Serializers
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.uuid.Uuids
import scala.util.control.NonFatal

import akka.stream.SystemMaterializer

/**
 */
object EventsByTagMigrationSpec {

  val config = ConfigFactory.parseString(s"""
  
       // disable normal failure logging as tall these tests are related 
       // so if one fails need the logs for all
       akka.loggers = []
       akka.loglevel = DEBUG
       akka {
         actor.serialize-messages=off
         actor.debug.unhandled = on
       }
       akka.persistence.cassandra {
         journal {
           keyspace-autocreate = true
           tables-autocreate = true
         }
         query {
           events-by-persistence-id-gap-timeout = 1s
         }
       }
    """).withFallback(CassandraLifecycle.config).withFallback(ConfigFactory.load())

}

class EventsByTagMigrationProvidePersistenceIds extends AbstractEventsByTagMigrationSpec {

  "Partial events by tag migration" must {
    val pidOne = "pOne"
    val pidTwo = "pTwo"

    "support migrating a subset of persistenceIds" in {
      writeOldTestEventWithTags(PersistentRepr("e-1", 1, pidOne), Set("blue"))
      writeOldTestEventWithTags(PersistentRepr("e-2", 2, pidOne), Set("blue"))
      writeOldTestEventWithTags(PersistentRepr("f-1", 1, pidTwo), Set("blue"))
      writeOldTestEventWithTags(PersistentRepr("f-2", 2, pidTwo), Set("blue"))

      migrator.createTables()
      migrator.addTagsColumn()

      migrator.migratePidsToTagViews(List(pidOne)).futureValue shouldEqual Done

      val blueSrc = queries.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])
      blueProbe.request(5)
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbe.expectNoMessage(waitTime)
      blueProbe.cancel()

      migrator.migratePidsToTagViews(List(pidTwo)).futureValue shouldEqual Done

      val blueSrcTakeTwo = queries.eventsByTag("blue", NoOffset)
      val blueProbeTakeTwo = blueSrcTakeTwo.runWith(TestSink.probe[Any])
      blueProbeTakeTwo.request(5)
      blueProbeTakeTwo.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbeTakeTwo.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbeTakeTwo.expectNextPF { case EventEnvelope(_, `pidTwo`, 1, "f-1") => }
      blueProbeTakeTwo.expectNextPF { case EventEnvelope(_, `pidTwo`, 2, "f-2") => }
      blueProbeTakeTwo.expectNoMessage(waitTime)
      blueProbeTakeTwo.cancel()
    }
  }
}

class EventsByTagMigrationSpec extends AbstractEventsByTagMigrationSpec {

  "Events by tag migration with no metadata" must {

    val pidOne = "p-1"
    val pidTwo = "p-2"
    val pidWithMeta = "pidMeta"
    val pidWithSnapshot = "pidSnapshot"
    val pidExcluded = "pidExcluded"

    "have some existing tagged messages" in {
      // this one uses the 0.7 schema, soo old.
      writeOldTestEventInMessagesColumn(PersistentRepr("e-1", 1L, pidOne), Set("blue", "green", "orange"))

      writeOldTestEventWithTags(PersistentRepr("e-2", 2L, pidOne), Set("blue"))
      writeOldTestEventWithTags(PersistentRepr("e-3", 3L, pidOne), Set())
      writeOldTestEventWithTags(PersistentRepr("e-4", 4L, pidOne), Set("blue", "green"))
      writeOldTestEventWithTags(PersistentRepr("f-1", 1L, pidTwo), Set("green"))
      writeOldTestEventWithTags(PersistentRepr("f-2", 2L, pidTwo), Set("blue"))
      writeOldTestEventWithTags(
        PersistentRepr("g-1", 1L, pidWithMeta),
        Set("blue"),
        Some("This is the best event ever"))

      // These events have been snapshotted
      writeOldTestEventWithTags(PersistentRepr("h-1", 10L, pidWithSnapshot), Set("red"))
      writeOldTestEventWithTags(PersistentRepr("h-2", 11L, pidWithSnapshot), Set("red"))
      writeToDeletedTo(pidWithSnapshot, 9)

      writeOldTestEventWithTags(PersistentRepr("i-1", 1L, pidExcluded), Set("bad-tag"))
    }

    "allow creation of the new tags view table" in {
      migrator.createTables().futureValue shouldEqual Done
    }

    "migrate tags to the new table" in {
      migrator.migrateToTagViews(filter = _ != pidExcluded).futureValue shouldEqual Done
    }

    "be idempotent so it can be restarted" in {
      // add some more events to be picked up
      writeOldTestEventWithTags(PersistentRepr("f-1", 1L, pidTwo), Set("green"))
      writeOldTestEventWithTags(PersistentRepr("f-2", 2L, pidTwo), Set("blue"))
      writeOldTestEventWithTags(
        PersistentRepr("g-1", 1L, pidWithMeta),
        Set("blue"),
        Some("This is the best event ever"))
    }

    "allow a second migration to resume from where the last one got to" in {
      migrator.migrateToTagViews(filter = _ != pidExcluded).futureValue shouldEqual Done
    }

    "migrate events missed during the large migration as part of actor recovery" in {
      // these events mimic the old version still running and persisting events
      writeOldTestEventWithTags(PersistentRepr("f-3", 3L, pidTwo), Set("green"))
      writeOldTestEventWithTags(PersistentRepr("f-4", 4L, pidTwo), Set("blue"))
    }

    "allow adding of the new tags column" in {
      migrator.addTagsColumn().futureValue shouldEqual Done
    }

    "work with the current implementation" in {
      val blueSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])
      blueProbe.request(5)
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 4, "e-4") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 2, "f-2") => }
      blueProbe.expectNextPF {
        case EventEnvelope(_, `pidWithMeta`, 1, EventWithMetaData("g-1", "This is the best event ever")) =>
      }
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

      val redSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("red", NoOffset)
      val redProbe = redSrc.runWith(TestSink.probe[Any])
      redProbe.request(3)
      redProbe.expectNextPF { case EventEnvelope(_, `pidWithSnapshot`, 10, "h-1") => }
      redProbe.expectNextPF { case EventEnvelope(_, `pidWithSnapshot`, 11, "h-2") => }
      redProbe.cancel()

      val excludedSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("bad-tag", NoOffset)
      val excludedProbe = excludedSrc.runWith(TestSink.probe[Any])
      excludedProbe.request(1)
      excludedProbe.expectNoMessage(waitTime)
      excludedProbe.cancel()
    }

    "see events missed by migration if the persistent actor is started" in {
      val probe = TestProbe()
      systemTwo.actorOf(TestTaggingActor.props(pidTwo, probe = Some(probe.ref)))
      probe.expectMsg(RecoveryCompleted)

      val blueSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])
      blueProbe.request(6)
      blueProbe.expectNextN(5) // ignore the ones we've already validated
      // This event wasn't migrated, should have been fixed on actor start up
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 4, "f-4") => }
      blueProbe.expectNoMessage(waitTime)
      blueProbe.cancel()

      val greenSrc: Source[EventEnvelope, NotUsed] = queries.eventsByTag("green", NoOffset)
      val greenProbe = greenSrc.runWith(TestSink.probe[Any])
      greenProbe.request(6)
      greenProbe.expectNextN(3) // ignore the ones we've already validated
      // This event wasn't migrated, should have been fixed on actor start up
      greenProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 3, "f-3") => }
      greenProbe.expectNoMessage(waitTime)
      greenProbe.cancel()

    }
    // This will be left as a manual step for the user as it stops
    // rolling back to the old version
    "allow dropping of the materialized view" in {
      system.log.info("Dropping old materialzied view")
      cluster.execute(SimpleStatement.newInstance(s"DROP MATERIALIZED VIEW $eventsByTagViewName"))
      system.log.info("Dropped old materialzied view")
    }

    "have a peek in the messages table" in {
      val row = cluster.execute(SimpleStatement.newInstance(s"select * from ${messagesTableName} limit 1")).one()
      system.log.debug("New messages table looks like: {}", row)
      system.log.debug("{}", row.getColumnDefinitions)
    }

    "be able to add tags to existing pids" in {
      // we need a new actor system for this as the old one will have prepared the statements without
      // the tags column existing
      val pidOnePA = systemTwo.actorOf(TestTaggingActor.props(pidOne, Set("blue", "yellow")))
      pidOnePA ! "new-event-1"
      expectMsg(1.second, Ack)
      pidOnePA ! "new-event-2"
      expectMsg(Ack)

      val blueSrc: Source[EventEnvelope, NotUsed] = queriesTwo.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])(SystemMaterializer(systemTwo).materializer)
      blueProbe.request(10)
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 2, "e-2") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 4, "e-4") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 2, "f-2") => }
      blueProbe.expectNextPF {
        case EventEnvelope(_, `pidWithMeta`, 1, EventWithMetaData("g-1", "This is the best event ever")) =>
      }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 4, "f-4")         => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 5, "new-event-1") => }
      blueProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 6, "new-event-2") => }
      blueProbe.expectNoMessage(waitTime)
      blueProbe.cancel()
      pidOnePA ! PoisonPill
    }

    // Again a manual step, leaving them is only wasting disk space
    // the new version will work with these columns still there
    "allow dropping of tag columns" in {
      cluster.execute(s"ALTER TABLE ${messagesTableName} DROP tag1")
      cluster.execute(s"ALTER TABLE ${messagesTableName} DROP tag2")
      cluster.execute(s"ALTER TABLE ${messagesTableName} DROP tag3")
    }

    "still work after dropping the tag columns" in {
      val pidTwoPA = systemThree.actorOf(TestTaggingActor.props(pidTwo, Set("orange")))
      pidTwoPA ! "new-event-1"
      expectMsg(Ack)
      pidTwoPA ! "new-event-2"
      expectMsg(Ack)

      val orangeSrc: Source[EventEnvelope, NotUsed] = queriesThree.eventsByTag("orange", NoOffset)
      val orangeProbe = orangeSrc.runWith(TestSink.probe[Any])(SystemMaterializer(systemThree).materializer)
      orangeProbe.request(3)
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidOne`, 1, "e-1")         => }
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 5, "new-event-1") => }
      orangeProbe.expectNextPF { case EventEnvelope(_, `pidTwo`, 6, "new-event-2") => }
      orangeProbe.expectNoMessage(waitTime)
      orangeProbe.expectNoMessage(waitTime)
      orangeProbe.cancel()
      pidTwoPA ! PoisonPill
    }
  }
}

abstract class AbstractEventsByTagMigrationSpec
    extends CassandraSpec(EventsByTagMigrationSpec.config)
    with DirectWriting
    with BeforeAndAfterAll {

  val messagesTableName = s"$journalName.messages"
  val eventsByTagViewName = s" $journalName.eventsByTag1"

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

  val oldMateterializedView =
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
       |CREATE KEYSPACE IF NOT EXISTS $journalName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }
     """.stripMargin

  val settings = new PluginSettings(system, system.settings.config.getConfig("akka.persistence.cassandra"))
  val statements = new CassandraJournalStatements(settings)

  val waitTime = 100.millis

  // if this uses the main actor system the PersistentQuery can see the
  // new table before it is dropped and re-created as the old table and get
  // very confused
  lazy val migrationSystem = ActorSystem("Migrator", system.settings.config)
  lazy val migrator = EventsByTagMigration(migrationSystem)

  // Lazy so they don't get created until the schema changes have happened
  lazy val systemTwo = ActorSystem("EventsByTagMigration-2", system.settings.config)
  lazy val queriesTwo =
    PersistenceQuery(systemTwo).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // for after tag1-3 columns are dropped
  lazy val systemThree = ActorSystem("EventsByTagMigration-3", system.settings.config)
  lazy val queriesThree =
    PersistenceQuery(systemThree).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    system.log.debug("Creating old tables, first dropping {}", messagesTableName)
    // Drop the messages table as we want to start with the old one
    cluster.execute(s"drop table $messagesTableName")
    cluster.execute(oldMessagesTable)
    cluster.execute(oldMateterializedView)
    system.log.debug("Old tables created")
  }

  private lazy val serialization = SerializationExtension(system)

  override protected def afterAll(): Unit = {
    try {
      externalCassandraCleanup()
      cluster.close()
    } catch {
      case NonFatal(e) =>
        println("Failed to cleanup cassandra")
        e.printStackTrace()
    }
    super.afterAll()
    shutdown(systemTwo)
    shutdown(systemThree)
  }

  // Write used before 0.80
  private def writeMessage(withMeta: Boolean) =
    s"""
      INSERT INTO ${messagesTableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event,
        ${if (withMeta) "meta_ser_id, meta_ser_manifest, meta," else ""}
        tag1, tag2, tag3, used)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ${if (withMeta) "?, ?, ?, " else ""} true)
    """

  private lazy val preparedWriteMessageWithMeta = cluster.prepare(writeMessage(true))

  private lazy val preparedWriteMessageWithoutMeta = cluster.prepare(writeMessage(false))

  private lazy val preparedWriteDeletedTo = cluster.prepare(statements.insertDeletedTo)

  private val writeMessageVersion0p7 =
    s"""
      INSERT INTO ${messagesTableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, tag1, tag2, tag3, message, used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true)

    """

  private lazy val preparedWriteVersion0p7 = cluster.prepare(writeMessageVersion0p7)

  def writeOldTestEventInMessagesColumn(pr: PersistentRepr, tags: Set[String]): Unit = {
    require(tags.size <= 3)
    val nowUuid = Uuids.timeBased()
    val now = Uuids.unixTimestamp(nowUuid)
    val bytes: Array[Byte] = serialization.serialize(pr).get

    val bound = tags.zipWithIndex
      .foldLeft(preparedWriteVersion0p7.bind()) {
        case (acc, (tag, index)) =>
          acc.setString(s"tag${index + 1}", tag)
      }
      .setString("persistence_id", pr.persistenceId)
      .setLong("partition_nr", 0L)
      .setLong("sequence_nr", pr.sequenceNr)
      .setUuid("timestamp", nowUuid)
      .setString("timebucket", TimeBucket(now, Hour).key.toString)
      .setByteBuffer("message", ByteBuffer.wrap(bytes))

    cluster.execute(bound)
  }

  def writeToDeletedTo(persistenceId: String, deletedTo: Long): Unit =
    cluster.execute(preparedWriteDeletedTo.bind(persistenceId, deletedTo: JLong))

  def writeOldTestEventWithTags(
      persistent: PersistentRepr,
      tags: Set[String],
      metadata: Option[String] = None): Unit = {
    require(tags.size <= 3)
    val event = persistent.payload.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(event)
    val serialized = ByteBuffer.wrap(serialization.serialize(event).get)

    val serManifest = Serializers.manifestFor(serializer, persistent)

    val ps = if (metadata.isDefined) preparedWriteMessageWithMeta else preparedWriteMessageWithoutMeta
    val nowUuid = Uuids.timeBased()
    val now = Uuids.unixTimestamp(nowUuid)
    val bound = tags.zipWithIndex
      .foldLeft(ps.bind()) {
        case (acc, (tag, index)) =>
          acc.setString(s"tag${index + 1}", tag)
      }
      .setString("persistence_id", persistent.persistenceId)
      .setLong("partition_nr", 0L)
      .setLong("sequence_nr", persistent.sequenceNr)
      .setUuid("timestamp", nowUuid)
      .setString("timebucket", TimeBucket(now, Hour).key.toString)
      .setInt("ser_id", serializer.identifier)
      .setString("ser_manifest", serManifest)
      .setString("event_manifest", persistent.manifest)
      .setByteBuffer("event", serialized)

    val finished = metadata match {
      case Some(m) =>
        val meta = m.asInstanceOf[AnyRef]
        val metaSerialiser = serialization.findSerializerFor(meta)
        val metaSerialised = ByteBuffer.wrap(serialization.serialize(meta).get)
        val serManifest = Serializers.manifestFor(serializer, meta)
        bound
          .setByteBuffer("meta", metaSerialised)
          .setInt("meta_ser_id", metaSerialiser.identifier)
          .setString("meta_ser_manifest", serManifest)
      case None =>
        bound

    }
    cluster.execute(finished)
    system.log.debug("Directly wrote payload [{}] for entity [{}]", persistent.payload, persistent.persistenceId)
  }

}
