package akka.persistence.cassandra.query

private[query] trait CassandraReadStatements {

  def config: CassandraReadJournalConfig

  def eventsByTagViewName = s"${config.keyspace}.${config.eventsByTagView}"

  def selectEventsByTag(tagId: Int) = s"""
      SELECT * FROM $eventsByTagViewName$tagId WHERE
        tag$tagId = ? AND
        timebucket = ? AND
        timestamp > ? AND
        timestamp <= ?
        ORDER BY timestamp ASC
        LIMIT ?
    """

}
