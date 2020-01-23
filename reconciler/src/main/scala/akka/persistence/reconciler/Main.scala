/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.ActorSystem
import akka.persistence.cassandra.journal.TagWriter._
import akka.persistence.cassandra.journal.TagWriters._
import akka.persistence.cassandra.journal.TagWriters

/**
 * Utility to reconcile the data created by the Akka Persistence Cassandra plugin.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val system: ActorSystem = ActorSystem("Reconciler")

    val tagWriterSettings: TagWriterSettings = null
    val tagWriterSession: TagWritersSession = null

    val tagWriters = system.actorOf(TagWriters.props(tagWriterSettings, tagWriterSession))

    Thread.sleep(5000)

    system.terminate()
  }
}
