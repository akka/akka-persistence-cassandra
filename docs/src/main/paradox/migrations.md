# Migration

## Migrating from 0.80+ to 1.0

* Rolling update to 1.0 requires update from 0.101 to 1.0. First update to 0.101.
* After being deprecated, the DateTieredCompactionStrategy has been removed.

### Driver upgrade with significant configuration changes

Version 4.x of the cassandra driver comes with a new way to configure it via [typesafe configuration](https://github.com/lightbend/config)
which matches how Akka manages configuration.

All driver related configuration e.g. query consistency, query retries etc has been removed from this
project's `reference.conf` and now each part of the plugin (journal, snapshot and query) specify a read and write 
@extref:[execution profile](java-driver:manual/core/configuration/#execution-profiles) that gives
fine grained control over consistencies and retires for each are. By default all read/write profiles are the same and under
`datastax-java-driver.profile.akka-persistence-cassandra-profile`. The only value in the profile provided by the plugin is setting the `basic.request.consistency`
to `QUORUM`.

The new driver supports reconnection during initialization which was previously built into the plugin. It is recommended to turn this on with:
`datastax-java-driver.advanced.reconnect-on-init = true`
It can't be turned on by the plugin as it is in the driver's reference.conf and is not overridable in a profile.

### Changed configuration structure

In addition to the driver related configuration described above the overall configuration structure has been changed.
It is now structured in four main sections within the top level `akka.persistence.cassandra` section:

    akka.persistence.cassandra {
      journal {
      }
      query {
      }
      events-by-tag {
      }
      snapshot {
      }
    }

See [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/main/core/src/main/resources/reference.conf)
for details and update your `application.conf` accordingly.

This also means that the properties for enabling the plugin have changed to:

    akka.persistence.journal.plugin = "akka.persistence.cassandra.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.cassandra.snapshot"

#### Removals

Using DateTieredCompactionStrategy with automatic schema creation

### Drop static column

See instructions for migrations to 0.101 below. To support rolling update if you are migrating from an
earlier version to 1.0 you must first update to 0.101+ but the second step can be skipped and will be part of the
1.0 update instead.

After completed update to 1.0 the static column `used` should be dropped with:

```
alter table akka.messages drop used;
```

### All persistenceIds query

The implementation of `persistenceIds` and `currentPersistenceIds` queries have been made more efficient
by inserting new persistence ids into a new table `all_persistence_ids`.

Create the `all_persistence_ids` table if you are not using `tables-autocreate=on`, which is not recommended for
production. See @ref:[table definition](journal.md#schema).

The following migration step is not needed if you don't use the `persistenceIds` or `currentPersistenceIds` queries.

Already existing persistence ids should be inserted into the new table. This can be done with the `Reconciliation`
tool:

@@snip [reconciler](/core/src/test/scala/doc/reconciler/AllPersistenceIdsMigrationCompileOnly.scala) { #imports #migrate}

You can run that migration tool while the old (or new) system is running, and it can be run several times if needed.  

## Migrations to 0.101 and later in 0.x series


Versions 0.101+ make it possible to drop the static column `used`.  This saves space for persistence ids
that have been deleted. Also some cloud Cassandra versions do not support static columns.

The complete removal of the static column must be performed in two steps to support rolling update where an
Akka Cluster is running a mix of versions prior to 0.101 and 0.101+.

If you can accept a full cluster shutdown you can update to the second step directly.

**Step 1**

0.101 is not using the static column `used` in the reads but still populates it in the writes so that earlier
versions can read it. First upgrade to a 0.x version greater than 0.101.

**Step 2**

After complete roll out of a 0.101+ version in step 1 the configuration can be changed so that the static column isn't used
at all.

```
cassandra-journal.write-static-column-compat = off
```

It can still run with an old schema where the column exists. 

After completed update of the configuration change the column can be dropped with:

```
alter table akka.messages drop used;
```  

## Migrations to 0.80 and later in 0.x series

0.80 introduces a completely different way to manage tags for events. You can skip right ahead to 0.98 without going to
0.80.

If you migrate directly to 1.0.0 you must first run the migration of the @ref:[All persistenceIds query](#all-persistenceids-query).

It is very important that you test this migration in a pre-production environment as once you drop the materialized view
and tag columns you can not roll back.
 
Most of the migration can be done while the old version of your application is running. The remaining steps happen automatically
when a persistent actor is recovered which may slow down actor recovery but only by a small amount if you follow the offline steps.
 
The `EventsByTagMigration` class provides a set of tools to assist in the migration.

The first two are schema changes that should be performed once on a single node 
and can be done while your application is running with the old version of this plugin:

* `createTables` creates the two new tables required
* `addTagsColumn` adds a `set<text>` column to the `messages` table 

For example you could put the following in a main method to do the schema migrations:

```scala
val system = ActorSystem()

val migrator = EventsByTagMigration(system)

val schemaMigration: Future[Done] = for {
  _ <- migrator.createTables()
  done <- migrator.addTagsColumn().recover { 
    case i: ExecutionException if i.getMessage.contains("conflicts with an existing column") => Done
  }
} yield done
```

The recover makes the adding of the tags column idempotent as cql does not include a `IF NOT EXISTS` for adding a column
 
Next is the data migration into a new table that stores events indexed by tag. This will be a slow process as it needs
to scan over all existing events. It can be run while your application is running but beware that it will
produce a lot of writes to your C* cluster so should be done during a quiet period.

* `migrateToTagViews` scans over all your persistence ids and writes the tagged events to the `tag_views` table. At the same
time it keeps track of its progress in the `tag_write_progress` table so if this were to fail due to say a C* issue you can resume
and it won't start from scratch.
* If you have an efficient way of getting all the persistenceIds pass them into this method. Otherwise a select distinct query is used
which is likely to time out. You can also use this method to stagger your migration.

You do not need to worry if a small number of events are missed by `migrateToTagViews` as they will be
fixed during your `PersistentActor` recovery. However do not rely on this for full migration as only active `PersistentActor`s
will be recovered and it will mean the start up time for your `PersistentActor`s will be very long.
Moreover, if your application consumes events through `eventsByTag` query, the missed events will only show up on the query side once you instantiate the respective `PersistentActor`. 

After you have migrated your data you can now remove the materialized view and `tagN` columns from the 
`messages` table. It is *highly* recommended you do this as maintaining a materialized view is expensive
and uses a large amount of capacity in your cluster:

* Drop the materialized view
```cql
DROP MATERIALIZED VIEW akka.eventsbytag;
```
* Drop the tag columns on the messages table
```cql
ALTER TABLE akka.messages DROP tag1;
ALTER TABLE akka.messages DROP tag2;
ALTER TABLE akka.messages DROP tag3;
```

Note that the new `tags` column won't be back filled for old events. This won't affect 
your `eventsByTag` queries as they come from a different table. This column is used for recovering any missed
writes to the `tag_views` table when running persistent actors with the new version.

The following configuration changes. See `reference.conf` for full details:

* `pubsub-minimum-interval` has been removed. If using DistributedPubsub notifications for tag writes then set `pubsub-notification` to on
* `delayed-event-timeout` has been replaced by `events-by-tag.gap-timeout` with the restriction removed that all
events have to be tagged. 
* `first-time-bucket` format has changed to: `yyyyMMddTHH:mm` e.g. `20151120T00:00`
* `eventual-consistency` has been removed. It may be re-added see [#263](https://github.com/akka/akka-persistence-cassandra/issues/263)

## Migrations from 0.54 to 0.59

In version 0.55 additional columns were added to be able to store meta data about an event without altering
the actual domain event.

The new columns `meta_ser_id`, `meta_ser_manifest`, and `meta` are defined in the [new journal table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.55/core/src/main/scala/akka/persistence/cassandra/journal/CassandraStatements.scala#L45-L47) and [new snapshot table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.55/core/src/main/scala/akka/persistence/cassandra/snapshot/CassandraStatements.scala#L31-L33).

You can add the to existing tables by executing the following in `cqlsh`:

```
alter table akka.messages add meta_ser_id int;
alter table akka.messages add meta_ser_manifest text;
alter table akka.messages add meta blob;
alter table akka_snapshot.snapshots add meta_ser_id int;
alter table akka_snapshot.snapshots add meta_ser_manifest text;
alter table akka_snapshot.snapshots add meta blob;
```

These columns are used when the event is wrapped in `akka.persistence.cassandra.EventWithMetaData` or snapshot is wrapped in `akka.persistence.cassandra.SnapshotWithMetaData`. It is optional to alter the table and add the columns. It's only required to add the columns if such meta data is used.

It is also not required to add the materialized views, not even if the meta data is stored in the journal table. If the materialized view is not changed the plain events are retrieved with the `eventsByTag` query and they are not wrapped in `EventWithMetaData`. Note that Cassandra [does not support](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlAlterMaterializedView.html) adding columns to an existing materialized view.

If you don't alter existing messages table and still use `tables-autocreate=on` you have to set config:

```
cassandra-journal.meta-in-events-by-tag-view = off
``` 

When trying to create the materialized view (tables-autocreate=on) with the meta columns before corresponding columns have been added the messages table an exception "Undefined column name meta_ser_id" is raised, because Cassandra validates the ["CREATE MATERIALIZED VIEW IF NOT EXISTS"](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCreateMaterializedView.html#cqlCreateMaterializedView__if-not-exists) even though the view already exists and will not be created. To work around that issue you can disable the meta columns in the materialized view by setting `meta-in-events-by-tag-view=off`.


## Migrations from 0.51 to 0.52

`CassandraLauncher` has been pulled out into its own artifact, and now bundles Cassandra into a single fat jar, which is bundled into the launcher artifact. This has allowed Cassandra to be launched without it being on the classpath, which prevents classpath conflicts, but it also means that Cassandra can't be configured by changing files on the classpath, for example, a custom `logback.xml` in `src/test/resources` is no longer sufficient to configure Cassandra's logging. To address this, `CassandraLauncher.start` now accepts a list of classpath elements that will be added to the classpath, and provides a utility for locating classpath elements based on resource name.

To depend on the new `CassandraLauncher` artifact, remove any dependency on `cassandra-all` itself, and add:

```scala
"com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.52"
```

to your build.  To modify the classpath that Cassandra uses, for example, if you have a `logback.xml` file in your `src/test/resources` directory that you want Cassandra to use, you can do this:

```scala
CassandraLauncher.start(
   cassandraDirectory,
   CassandraLauncher.DefaultTestConfigResource,
   clean = true,
   port = 0,
   CassandraLauncher.classpathForResources("logback.xml")
)
```

## Migrations from 0.23 to 0.50

The Persistence Query API changed slightly, see [migration guide for Akka 2.5](https://doc.akka.io/docs/akka/2.5/project/migration-guide-2.4.x-2.5.x.html#persistence-query).

## Migrations from 0.11 to 0.12

Dispatcher configuration was changed, see [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/main/core/src/main/resources/reference.conf):

## Migrations from 0.9 to 0.10

The event data, snapshot data and meta data are stored in a separate columns instead of being wrapped in blob. Run the following statements in `cqlsh`:

    drop materialized view if exists akka.eventsbytag1;
    drop materialized view if exists akka.eventsbytag2;
    drop materialized view if exists akka.eventsbytag3;
    alter table akka.messages add writer_uuid text;
    alter table akka.messages add ser_id int;
    alter table akka.messages add ser_manifest text;
    alter table akka.messages add event_manifest text;
    alter table akka.messages add event blob;
    alter table akka_snapshot.snapshots add ser_id int;
    alter table akka_snapshot.snapshots add ser_manifest text;
    alter table akka_snapshot.snapshots add snapshot_data blob;

## Migrations from 0.6 to 0.7

Schema changes mean that you can't upgrade from version 0.6 for Cassandra 2.x of the plugin to the 0.7 version and use existing data without schema migration. You should be able to export the data and load it to the [new table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.7/src/main/scala/akka/persistence/cassandra/journal/CassandraStatements.scala#L25).

## Migrating from 0.3.x (Akka 2.3.x)

Schema and property changes mean that you can't currently upgrade from 0.3 to 0.4 SNAPSHOT and use existing data without schema migration. You should be able to export the data and load it to the [new table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.9/src/main/scala/akka/persistence/cassandra/journal/CassandraStatements.scala).

