package akka.persistence.journal.cassandra

class CassandraStatements(keyspace: String, table: String) {
  private val tableName = s"${keyspace}.${table}"

  def createKeyspace(replicationFactor: Int) = s"""
      CREATE KEYSPACE IF NOT EXISTS ${keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }
    """

  val createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        processor_id text,
        sequence_nr bigint,
        marker text,
        message blob,
        PRIMARY KEY (processor_id, sequence_nr, marker))
    """

  val insertMessage = s"""
      INSERT INTO ${tableName} (processor_id, sequence_nr, marker, message)
      VALUES (?, ?, 'A', ?)
    """

  val confirmMessage = s"""
      INSERT INTO ${tableName} (processor_id, sequence_nr, marker)
      VALUES (?, ?, ?)
    """

  val deleteMessageLogical = s"""
      INSERT INTO ${tableName} (processor_id, sequence_nr, marker)
      VALUES (?, ?, 'B')
    """

  val deleteMessagePermanent = s"""
      DELETE FROM ${tableName} WHERE
        processor_id = ? AND
        sequence_nr = ?
    """

  val selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        processor_id = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """
}
