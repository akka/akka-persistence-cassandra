/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.Done
import com.datastax.driver.core.Session
import akka.persistence.cassandra.session.scaladsl.CassandraSession

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createConfigTable = s"""
      CREATE TABLE IF NOT EXISTS ${configTableName} (
        property text primary key, value text)
     """

  // message is the serialized PersistentRepr that was used in v0.6 and earlier
  // event is the serialized application event that is used in v0.7 and later
  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        used boolean static,
        persistence_id text,
        partition_nr bigint,
        sequence_nr bigint,
        timestamp timeuuid,
        timebucket text,
        writer_uuid text,
        ser_id int,
        ser_manifest text,
        event_manifest text,
        event blob,
        tag1 text,
        tag2 text,
        tag3 text,
        message blob,
        PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
        WITH gc_grace_seconds =${config.gcGraceSeconds}
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def createMetatdataTable = s"""
      CREATE TABLE IF NOT EXISTS ${metadataTableName}(
        persistence_id text PRIMARY KEY,
        deleted_to bigint,
        properties map<text,text>)
   """

  def createEventsByTagMaterializedView(tagId: Int) = s"""
      CREATE MATERIALIZED VIEW IF NOT EXISTS $eventsByTagViewName$tagId AS
         SELECT tag$tagId, timebucket, timestamp, persistence_id, partition_nr, sequence_nr, writer_uuid, ser_id, ser_manifest, event_manifest, event, message
         FROM $tableName
         WHERE persistence_id IS NOT NULL AND partition_nr IS NOT NULL AND sequence_nr IS NOT NULL
           AND tag$tagId IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
         PRIMARY KEY ((tag$tagId, timebucket), timestamp, persistence_id, partition_nr, sequence_nr)
         WITH CLUSTERING ORDER BY (timestamp ASC)
      """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event, used)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? , true)
    """

  def deleteMessage = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def deleteMessages = {
    if (config.cassandra2xCompat)
      s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """
    else
      s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr <= ?
    """
  }

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """

  def selectInUse = s"""
     SELECT used from ${tableName} WHERE
      persistence_id = ? AND
      partition_nr = ?
   """
  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?) IF NOT EXISTS
    """

  def selectHighestSequenceNr = s"""
     SELECT sequence_nr, used FROM ${tableName} WHERE
       persistence_id = ? AND
       partition_nr = ?
       ORDER BY sequence_nr
       DESC LIMIT 1
   """

  def selectDeletedTo = s"""
      SELECT deleted_to FROM ${metadataTableName} WHERE
        persistence_id = ?
    """

  def insertDeletedTo = s"""
      INSERT INTO ${metadataTableName} (persistence_id, deleted_to)
      VALUES ( ?, ? )
    """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (persistence_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
  private def eventsByTagViewName = s"${config.keyspace}.${config.eventsByTagView}"

  /**
   * Execute creation of keyspace and tables is limited to one thread at a time
   * reduce the risk of (annoying) "Column family ID mismatch" exception
   * when write and read-side plugins are started at the same time.
   * Those statements are retried, because that could happen across different
   * nodes also but serializing those statements gives a better "experience".
   *
   * The materialized view for eventsByTag query is not created if `maxTagId` is 0.
   */
  def executeCreateKeyspaceAndTables(session: Session, config: CassandraJournalConfig,
                                     maxTagId: Int)(implicit ec: ExecutionContext): Future[Done] = {
    import akka.persistence.cassandra.listenableFutureToFuture

    def create(): Future[Done] = {

      val keyspace: Future[Done] =
        if (config.keyspaceAutoCreate) session.executeAsync(createKeyspace).map(_ => Done)
        else Future.successful(Done)

      val tables =
        if (config.tablesAutoCreate) {
          for {
            _ <- keyspace
            _ <- session.executeAsync(createTable)
            _ <- session.executeAsync(createMetatdataTable)
            done <- session.executeAsync(createConfigTable).map(_ => Done)
          } yield done
        } else keyspace

      def createTag(todo: List[String]): Future[Done] =
        todo match {
          case create :: remainder => session.executeAsync(create).flatMap(_ => createTag(remainder))
          case Nil                 => Future.successful(Done)
        }

      if (config.tablesAutoCreate) {
        val createTagStmts = (1 to maxTagId).map(createEventsByTagMaterializedView).toList
        tables.flatMap(_ => createTag(createTagStmts))
      } else tables
    }

    CassandraSession.serializedExecution(
      recur = () => executeCreateKeyspaceAndTables(session, config, maxTagId),
      exec = () => create()
    )
  }

  def initializePersistentConfig(session: Session)(implicit ec: ExecutionContext): Future[Map[String, String]] = {
    import akka.persistence.cassandra.listenableFutureToFuture
    session.executeAsync(selectConfig)
      .flatMap { rs =>
        val properties = rs.all.asScala.map(row => (row.getString("property"), row.getString("value"))).toMap
        properties.get(CassandraJournalConfig.TargetPartitionProperty) match {
          case Some(oldValue) =>
            assertCorrectPartitionSize(oldValue)
            Future.successful(properties)
          case None =>
            session.executeAsync(writeConfig, CassandraJournalConfig.TargetPartitionProperty, config.targetPartitionSize.toString)
              .map { rs =>
                if (!rs.wasApplied())
                  Option(rs.one).map(_.getString("value")).foreach(assertCorrectPartitionSize)
                properties.updated(CassandraJournalConfig.TargetPartitionProperty, config.targetPartitionSize.toString)
              }
        }
      }

  }

  private def assertCorrectPartitionSize(size: String) =
    require(size.toInt == config.targetPartitionSize, "Can't change target-partition-size")
}
