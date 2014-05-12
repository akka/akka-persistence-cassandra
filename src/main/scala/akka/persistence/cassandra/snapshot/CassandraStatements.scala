package akka.persistence.cassandra.snapshot

trait CassandraStatements {
  def keyspace: String
  def table: String

  def createKeyspace(replicationFactor: Int) = s"""
      CREATE KEYSPACE IF NOT EXISTS ${keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        processor_id text,
        sequence_nr bigint,
        timestamp bigint,
        snapshot blob,
        PRIMARY KEY (processor_id, sequence_nr))
        WITH CLUSTERING ORDER BY (sequence_nr DESC)
    """

  def writeSnapshot = s"""
      INSERT INTO ${tableName} (processor_id, sequence_nr, timestamp, snapshot)
      VALUES (?, ?, ?, ?)
    """

  def deleteSnapshot = s"""
      DELETE FROM ${tableName} WHERE
        processor_id = ? AND
        sequence_nr = ?
    """

  def selectSnapshot = s"""
      SELECT * FROM ${tableName} WHERE
        processor_id = ? AND
        sequence_nr = ?
    """

  def selectSnapshotMetadata(limit: Option[Int] = None) = s"""
      SELECT processor_id, sequence_nr, timestamp FROM ${tableName} WHERE
        processor_id = ? AND
        sequence_nr <= ?
        ${limit.map(l => s"LIMIT ${l}").getOrElse("")}
    """

  private def tableName = s"${keyspace}.${table}"
}
