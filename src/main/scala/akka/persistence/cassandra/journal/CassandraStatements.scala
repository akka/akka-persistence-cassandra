package akka.persistence.cassandra.journal

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createTable = s"""
      CREATE TABLE ${tableName} (
        processor_id text,
        partition_nr bigint,
        sequence_nr bigint,
        marker text,
        message blob,
        PRIMARY KEY ((processor_id, partition_nr), sequence_nr, marker))
        WITH COMPACT STORAGE
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
