/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.persistence.cassandra.journal.CassandraJournal.Serialized
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.{ PreparedStatement, Statement }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait CassandraEventUpdate extends CassandraStatements {

  private[akka] val session: CassandraSession
  private[akka] def config: CassandraJournalConfig
  private[akka] implicit val ec: ExecutionContext

  def preparedUpdateMessage: Future[PreparedStatement] = session.prepare(updateMessagePayload).map(_.setIdempotent(true))

  /*
    FIXME, make the update in the tags table as well.
    Change API so have existing tags/new tags so we know whether to update
    existing rows or add new tags. Or perhaps a separate API for adding tags.


    Steps for updating an event.
    - Update it in the main table.
    - Find its tags, everything apart from the tag_pid_sequence_nr is known so find that first.
    - Issue updates to the tags table.
   */

  def updateEvent(event: Serialized): Future[Done] = {
    preparedUpdateMessage.flatMap {
      ps => session.executeWrite(prepareUpdate(ps, event))
    }
  }

  def addTags(event: Serialized, newTags: Set[String]): Future[Done] = ???


  private def prepareUpdate(ps: PreparedStatement, s: Serialized): Statement = {
    val maxPnr = partitionNr(s.sequenceNr, config.targetPartitionSize)
    val bs = ps.bind()

    // primary key
    bs.setString("persistence_id", s.persistenceId)
    bs.setLong("partition_nr", maxPnr)
    bs.setLong("sequence_nr", s.sequenceNr)
    bs.setUUID("timestamp", s.timeUuid)
    bs.setString("timebucket", s.timeBucket.key.toString)

    // fields to update
    bs.setInt("ser_id", s.serId)
    bs.setString("ser_manifest", s.serManifest)
    bs.setString("event_manifest", s.eventAdapterManifest)
    bs.setBytes("event", s.serialized)
    bs.setSet("tags", s.tags.asJava, classOf[String])
    bs
  }
}
