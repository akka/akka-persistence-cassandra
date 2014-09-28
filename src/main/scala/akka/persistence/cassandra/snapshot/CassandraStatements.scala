package akka.persistence.cassandra.snapshot

trait CassandraStatements {
  def config: CassandraSnapshotStoreConfig

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

  private def tableName = s"${config.keyspace}.${config.table}"
}
