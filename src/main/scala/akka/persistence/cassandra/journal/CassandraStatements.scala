package akka.persistence.cassandra.journal

trait CassandraStatements {
  def config: CassandraJournalConfig

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
