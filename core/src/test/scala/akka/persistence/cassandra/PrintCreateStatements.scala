/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.io.File
import java.io.PrintWriter

import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.snapshot.CassandraSnapshotStoreConfig
import akka.actor.ActorSystem

/**
 * Main application that prints the create keyspace and create table statements.
 * It's using `cassandra-plugin` and `cassandra-plugin.snapshot` configuration from default application.conf.
 *
 * These statements can be copy-pasted and run in `cqlsh`.
 */
object PrintCreateStatements {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PrintCreateStatements")

    def withWriter(name: String)(f: PrintWriter => Unit): Unit = {
      val writer: PrintWriter = new PrintWriter(new File(name))
      try {
        f(writer)
      } finally {
        writer.flush()
        writer.close()
      }

    }

    val journalSettings =
      new CassandraJournalConfig(system, system.settings.config.getConfig("cassandra-plugin"))
    withWriter("./target/journal-keyspace.txt") { pw =>
      pw.println("//#journal-keyspace")
      pw.println(journalSettings.createKeyspaceStatement + ";")
      pw.println("//#journal-keyspace")
    }
    withWriter("./target/journal-tables.txt") { pw =>
      pw.println("//#journal-tables")
      pw.println(journalSettings.createTablesStatements.mkString(";\n\n") + ";")
      pw.println("//#journal-tables")
    }

    val snapshotSettings =
      new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig("cassandra-plugin"))
    withWriter("./target/snapshot-keyspace.txt") { pw =>
      pw.println("//#snapshot-keyspace")
      pw.println(snapshotSettings.createKeyspaceStatement + ";")
      pw.println("//#snapshot-keyspace")
    }
    withWriter("./target/snapshot-tables.txt") { pw =>
      pw.println("//#snapshot-tables")
      pw.println(snapshotSettings.createTablesStatements.mkString(";\n\n") + ";")
      pw.println("//#snapshot-tables")
    }

    system.terminate()
  }

}
