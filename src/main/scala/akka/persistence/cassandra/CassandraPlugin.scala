package akka.persistence.cassandra

import akka.event.LoggingAdapter

import com.datastax.driver.core.Session
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

  def createKeyspace(session:Session) {
    if (config.configureKeyspace) {
      try {
        if (config.keyspaceStrategyClass == "NetworkTopologyStrategy")
          session.execute(createKeyspaceWithNetworkTopologyStrategy(config.keyspaceStrategyClassDC1Name, config.keyspaceStrategyClassDC1RepFactor, config.keyspaceStrategyClassDC2Name, config.keyspaceStrategyClassDC2RepFactor))
        else
          session.execute(createKeyspaceWithSimpleStrategy(config.replicationFactor))
      } catch {
        case ae: AlreadyExistsException => logger.info(s"Keyspace ${config.keyspace} not created as it already exists");
      }
    }
  }
  
  def createTable(session:Session, statement: String) {
    if (config.configureTable) {
      try {
        session.execute(statement)
      } catch {
        case ae: AlreadyExistsException => logger.info(s"Table not created as it already exists");
      }
    }
  }

  def tableName = s"${config.keyspace}.${config.table}"
}
