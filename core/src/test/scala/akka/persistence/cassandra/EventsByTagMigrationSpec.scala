/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike

/**
 */
object EventsByTagMigrationSpec {

  val keyspaceName = "EventsByTagMigrationn"
  val messagesTableName = s"$keyspaceName.messages"
  val eventsByTagViewName = s"$keyspaceName.eventsByTag1"

  val oldMessagesTable =
    s"""
       | CREATE TABLE IF NOT EXISTS $messagesTableName(
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
      CREATE MATERIALIZED VIEW IF NOT EXISTS $eventsByTagViewName AS
         SELECT tag1, timebucket, timestamp, persistence_id, partition_nr, sequence_nr, writer_uuid, ser_id, ser_manifest, event_manifest, event,
           meta_ser_id, meta_ser_manifest, meta, message
         FROM old_messages
         WHERE persistence_id IS NOT NULL AND partition_nr IS NOT NULL AND sequence_nr IS NOT NULL
           AND tag1 IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
         PRIMARY KEY ((tag1, timebucket), timestamp, persistence_id, partition_nr, sequence_nr)
         WITH CLUSTERING ORDER BY (timestamp ASC)
      """

  val config = ConfigFactory.parseString(
    """
      |
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

}

class EventsByTagMigrationSpec extends TestKit(ActorSystem("EventsByTagMigration"))
  with WordSpecLike
  with CassandraLifecycle {

  import EventsByTagMigrationSpec._

  override def systemName = "EventsByTagMigration"

  lazy val session = cluster.connect()

  override protected def beforeAll(): Unit = {
    session.execute(oldMessagesTable)
    session.execute(oldMaterlializedView)
    super.beforeAll()
  }

  // not implemented yet
  "Events by tag migration" ignore {
    "have some existing tagged messages" in {
      pending
    }

    "allow dropping of the materialized view" in {
      session.execute(s"DROP MATERIALIZED VIEW $eventsByTagViewName")
    }

    "allow dropping of tag columns" in {
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag1")
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag2")
      session.execute(s"ALTER TABLE ${messagesTableName} DROP tag3")
    }

    "allow adding of the new tags column" in {
      session.execute(s"ALTER TABLE ${messagesTableName} ADD tags set<text>;")
    }

    "migrate tags to the new table" in {
      pending
    }

    "work with the current implementation" in {
      val s2 = ActorSystem("EventsByTagMigration")
      pending
    }
  }
}
