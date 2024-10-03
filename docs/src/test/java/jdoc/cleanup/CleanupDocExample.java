package jdoc.cleanup;

import akka.Done;
import akka.actor.ActorSystem;
import akka.persistence.cassandra.cleanup.Cleanup;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.PersistenceQuery;

import scala.jdk.javaapi.FutureConverters;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class CleanupDocExample {



    public static void example() {

        ActorSystem system = null;

        //#cleanup
        CassandraReadJournal queries = PersistenceQuery.get(system).getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
        Cleanup cleanup = new Cleanup(system);

        int persistenceIdParallelism = 10;


        // forall persistence ids, keep two snapshots and delete all events before the oldest kept snapshot
        queries.currentPersistenceIds().mapAsync(persistenceIdParallelism, pid -> FutureConverters.asJava(cleanup.cleanupBeforeSnapshot(pid, 2))).run(system);

        // forall persistence ids, keep everything after the provided unix timestamp, if there aren't enough snapshots after this time
        // go back before the timestamp to find snapshot to delete before
        // this operation is more expensive that the one above
        ZonedDateTime keepAfter = ZonedDateTime.now().minus(1, ChronoUnit.MONTHS);
        queries
                .currentPersistenceIds()
                .mapAsync(persistenceIdParallelism, pid -> FutureConverters.asJava(cleanup.cleanupBeforeSnapshot(pid, 2, keepAfter.toInstant().toEpochMilli())))
                .run(system);

        //#cleanup


    }
}
