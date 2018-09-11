/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.snapshot.CassandraSnapshotStoreConfig

import akka.actor.ActorSystem

/**
 * Main application that prints the create keyspace and create table statements.
 * It's using `cassandra-journal` and `cassandra-snapshot-store` configuration from default application.conf.
 *
 * These statements can be copy-pasted and run in `cqlsh`.
 */
object PrintCreateStatements {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PrintCreateStatements")

    val journalSettings = new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-journal"))
    println(journalSettings.createKeyspaceStatement + ";")
    println("")
    println(journalSettings.createTablesStatements.mkString(";\n\n") + ";")
    println("")

    val snapshotSettings =
      new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig("cassandra-snapshot-store"))
    println(snapshotSettings.createKeyspaceStatement + ";")
    println("")
    println(snapshotSettings.createTablesStatements.mkString(";\n\n") + ";")
    println("")

    system.terminate()
  }

}
