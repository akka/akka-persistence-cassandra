package akka.persistence.cassandra.journal

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        processor_id text,
        partition_nr bigint,
        sequence_nr bigint,
        marker text,
        message blob,
        PRIMARY KEY ((processor_id, partition_nr), sequence_nr, marker))
        WITH COMPACT STORAGE
         AND gc_grace_seconds =${config.gc_grace_seconds}
    """

  def writeHeader = s"""
      INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, 0, 'H', 0x00)
    """

  def writeMessage = s"""
      INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, ?, 'A', ?)
    """

  def confirmMessage = s"""
      INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, ?, ?, 0x00)
    """

  def deleteMessageLogical = s"""
      INSERT INTO ${tableName} (processor_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, ?, 'B', 0x00)
    """

  def deleteMessagePermanent = s"""
      DELETE FROM ${tableName} WHERE
        processor_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def selectHeader = s"""
      SELECT * FROM ${tableName} WHERE
        processor_id = ? AND
        partition_nr = ? AND
        sequence_nr = 0
    """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        processor_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
        LIMIT ${config.maxResultSize}
    """

  private def tableName = s"${config.keyspace}.${config.table}"
}
