/*
 * Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.reconciler

//#imports
import scala.util.Failure
import scala.util.Success

import akka.actor.ActorSystem
import akka.persistence.cassandra.reconciler.Reconciliation

//#imports

class AllPersistenceIdsMigrationCompileOnly {

  //#migrate
  // System should have the same Cassandra plugin configuration as your application
  // but be careful to remove seed nodes so this doesn't join the cluster
  val system = ActorSystem()
  import system.dispatcher

  val rec = new Reconciliation(system)
  val result = rec.rebuildAllPersistenceIds()

  result.onComplete {
    case Success(_) =>
      system.log.info("All persistenceIds migrated.")
      system.terminate()
    case Failure(e) =>
      system.log.error(e, "All persistenceIds migration failed.")
      system.terminate()
  }
  //#migrate
}
