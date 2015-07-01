package akka.persistence.cassandra.journal

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

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        persistence_id text,
        partition_nr bigint,
        sequence_nr bigint,
        marker text,
        message blob,
        PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, marker))
        WITH COMPACT STORAGE
         AND gc_grace_seconds =${config.gc_grace_seconds}
    """

  def writeHeader = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, 0, 'H', 0x00)
    """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, ?, 'A', ?)
    """

  def confirmMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, marker, message)
      VALUES (?, ?, ?, ?, 0x00)
    """

  def deleteMessage = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def selectHeader = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = 0
    """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
        LIMIT ${config.maxResultSize}
    """

  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?)
    """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
}
