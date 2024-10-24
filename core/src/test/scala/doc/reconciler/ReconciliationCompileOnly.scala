/*
 * Copyright (C) 2016-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.reconciler

//#imports
import akka.persistence.cassandra.reconciler.Reconciliation
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.Done

//#imports

class ReconciliationCompileOnly {

  //#reconcile
  // System should have the same Cassandra plugin configuration as your application
  // but be careful to remove seed nodes so this doesn't join the cluster
  val system = ActorSystem()
  import system.dispatcher

  val rec = new Reconciliation(system)

  // Drop and re-create data for a persistence id
  val pid = "pid1"
  for {
    // do this before dropping the data
    tags <- rec.tagsForPersistenceId(pid)
    // drop the tag view for every tag for this persistence id
    dropData <- Future.traverse(tags)(tag => rec.deleteTagViewForPersistenceIds(Set(pid), tag))
    // optional: re-build, if this is ommited then it will be re-build next time the pid is started
    _ <- rec.rebuildTagViewForPersistenceIds(pid)
  } yield Done
  //#reconcile
}
