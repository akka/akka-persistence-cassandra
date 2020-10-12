package doc.cleanup

import java.time.{ LocalDate, ZonedDateTime }
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.persistence.cassandra.cleanup.Cleanup
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery

object CleanupDocExample {

  implicit val system: ActorSystem = ???

  //#cleanup
  val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
  val cleanup = new Cleanup(system)

  //  how many persistence ids to operate on in parallel
  val persistenceIdParallelism = 10

  // forall persistence ids, keep two snapshots and delete all events before the oldest kept snapshot
  queries.currentPersistenceIds().mapAsync(persistenceIdParallelism)(pid => cleanup.cleanupBeforeSnapshot(pid, 2)).run()

  // forall persistence ids, keep everything after the provided unix timestamp, if there aren't enough snapshots after this time
  // go back before the timestamp to find snapshot to delete before
  // this operation is more expensive that the one above
  val keepAfter = ZonedDateTime.now().minus(1, ChronoUnit.MONTHS);
  queries
    .currentPersistenceIds()
    .mapAsync(persistenceIdParallelism)(pid => cleanup.cleanupBeforeSnapshot(pid, 2, keepAfter.toInstant.toEpochMilli))
    .run()

  //#cleanup

}
