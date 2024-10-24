/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.io.File
import java.io.PrintWriter
import akka.actor.ActorSystem

/**
 * Main application that prints the create keyspace and create table statements.
 * It's using `akka.persistence.cassandra` configuration from default application.conf.
 *
 * These statements can be copy-pasted and run in `cqlsh`.
 */
object PrintCreateStatements {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PrintCreateStatements")
    val statements = new KeyspaceAndTableStatements(system, "akka.persistence.cassandra", PluginSettings(system))

    def withWriter(name: String)(f: PrintWriter => Unit): Unit = {
      val writer: PrintWriter = new PrintWriter(new File(name))
      try {
        f(writer)
      } finally {
        writer.flush()
        writer.close()
      }

    }

    withWriter("./target/journal-keyspace.txt") { pw =>
      pw.println("//#journal-keyspace")
      pw.println(statements.createJournalKeyspaceStatement + ";")
      pw.println("//#journal-keyspace")
    }

    withWriter("./target/journal-tables.txt") { pw =>
      pw.println("//#journal-tables")
      pw.println(statements.createJournalTablesStatements.mkString(";\n\n") + ";")
      pw.println("//#journal-tables")
    }

    withWriter("./target/snapshot-keyspace.txt") { pw =>
      pw.println("//#snapshot-keyspace")
      pw.println(statements.createSnapshotKeyspaceStatement + ";")
      pw.println("//#snapshot-keyspace")
    }
    withWriter("./target/snapshot-tables.txt") { pw =>
      pw.println("//#snapshot-tables")
      pw.println(statements.createSnapshotTablesStatements.mkString(";\n\n") + ";")
      pw.println("//#snapshot-tables")
    }

    system.terminate()
  }

}
