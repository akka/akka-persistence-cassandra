/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package doc.reconciler

//#imports
import akka.persistence.cassandra.reconciler.Reconciliation
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.Done

//#imports

class ReconciliationCompileOnly {

  //#continue
  // System should have the same Cassandra plugin configuration as your application
  // but be careful to remove seed nodes so this doesn't join the cluster
  val system = ActorSystem()
  import system.dispatcher

  val rec = new Reconciliation(system)

  // Continue pending tag writes after a node crash
  // This can be run while the actor is still running on another node
  val pid = "pid1"
  val result: Future[Done] = rec.continueTagWritesForPersistenceId(pid)
  //#continue

  //#reconcile
  // Drop and re-create data for a persistence id
  val pid2 = "pid2"
  for {
    // do this before dropping the data
    tags <- rec.tagsForPersistenceId(pid2)
    // drop the tag view for every tag for this persistence id
    dropData <- Future.traverse(tags)(tag => rec.deleteTagViewForPersistenceIds(Set(pid2), tag))
    // optional: re-build, if this is omitted then it will be re-built next time the pid is started
    _ <- rec.rebuildTagViewForPersistenceIds(pid2)
  } yield Done
  //#reconcile
}
