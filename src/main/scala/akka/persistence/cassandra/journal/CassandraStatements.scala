package akka.persistence.cassandra.journal

trait CassandraStatements {
  def keyspace: String
  def table: String
  def maxResultSize: Int

  def createKeyspace(replicationFactor: Int) = s"""
      CREATE KEYSPACE IF NOT EXISTS ${keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }
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
        LIMIT ${maxResultSize}
    """

  private def tableName = s"${keyspace}.${table}"
}
