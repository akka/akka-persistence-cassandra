package akka.persistence.cassandra

import com.typesafe.config.Config
import com.datastax.driver.core.Session
import akka.event.LoggingAdapter
import com.datastax.driver.core.exceptions.AlreadyExistsException

trait CassandraPlugin {
  def logger: LoggingAdapter
  def config: CassandraPluginConfig
  
  def createKeyspaceWithNetworkTopologyStrategy(DC1: String, DC1ReplicationFactor: Int, DC2: String, DC2ReplicatiobFactor: Int) = s"""
      CREATE KEYSPACE ${config.keyspace}
      WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '${DC1}' : ${DC1ReplicationFactor}, '${DC2}' : ${DC2ReplicatiobFactor} }
  """
  def createKeyspaceWithSimpleStrategy(replicationFactor: Int) = s"""
      CREATE KEYSPACE ${config.keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor}}
  """

  def createJournalTable = s"""
      CREATE TABLE ${tableName} (
        processor_id text,
        partition_nr bigint,
        sequence_nr bigint,
        marker text,
        message blob,
        PRIMARY KEY ((processor_id, partition_nr), sequence_nr, marker))
        WITH COMPACT STORAGE
    """
  
  def createSnapshotTable = s"""
      CREATE TABLE ${tableName} (
        processor_id text,
        sequence_nr bigint,
        timestamp bigint,
        snapshot blob,
        PRIMARY KEY (processor_id, sequence_nr))
        WITH CLUSTERING ORDER BY (sequence_nr DESC)
    """
  
  def createKeyspace(session:Session) {
    if(config.configureKeyspace){
      if(config.keyspaceStrategyClass == "NetworkTopologyStrategy"){
        try {
          session.execute(createKeyspaceWithNetworkTopologyStrategy(config.keyspaceStrategyClassDC1Name,config.keyspaceStrategyClassDC1RepFactor,config.keyspaceStrategyClassDC2Name,config.keyspaceStrategyClassDC2RepFactor))
          }catch {
            case ae: AlreadyExistsException => logger.info("Keyspace not created as it already exists : " + ae);
            case e: Exception => {
              logger.error("Unknown error while creating keyspace: " + e);
              throw e;
             }
          }
        }
      else {
        try {
          session.execute(createKeyspaceWithSimpleStrategy(config.replicationFactor))
          }catch {
            case ae: AlreadyExistsException => logger.info("Keyspace not created as it already exists : " + ae);
            case e: Exception => {
              logger.error("Unknown error while creating keyspace: " + e);
              throw e;
              }
            }
          }
      }
  }
  
  def createJournalTable(session:Session){
  if(config.configureTable){
    try {
      session.execute(createJournalTable)
      }catch {
        case ae: AlreadyExistsException => logger.info("Table not created as it already exists: " + ae);
        case e: Exception => {
          logger.error("Unknown error while creating table: " + e);
          throw e;
          }
        }
      }
  }
  
  def createSnapshotTable(session:Session){
  if(config.configureTable){
    try {
      session.execute(createSnapshotTable)
      }catch {
          case ae: AlreadyExistsException => logger.info("Table not created as it already exists: " + ae);
          case e: Exception => {
           logger.error("Unknown error while creating table: " + e);
           throw e;
          }
        }
      }
  }
  private def tableName = s"${config.keyspace}.${config.table}"
}
