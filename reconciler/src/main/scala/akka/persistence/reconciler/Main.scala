/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.reconciler

import akka.actor.typed.ActorSystem

/**
 * Utility to reconcile the data created by the Akka Persistence Cassandra plugin.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val system: ActorSystem[_] = ???
    system.terminate()
  }
}
