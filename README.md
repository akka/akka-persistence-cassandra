Cassandra Plugins for Akka Persistence
======================================

For questions please use the [discuss.akka.io](https://discuss.lightbend.com/c/akka/). Tag any new questions with `akka-persistence` and `cassandra`.

[![Join the chat at https://gitter.im/akka/akka-persistence-cassandra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akka/akka-persistence-cassandra?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Replicated [Akka Persistence](https://doc.akka.io/docs/akka/current/scala/persistence.html) journal and snapshot store backed by [Apache Cassandra](https://cassandra.apache.org/).

[![Build Status](https://travis-ci.org/akka/akka-persistence-cassandra.svg?branch=master)](https://travis-ci.org/akka/akka-persistence-cassandra)

Dependencies
------------

### Latest release

To include the latest release of the Cassandra plugins for **Akka 2.5.x** into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += Seq(
      "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.95",
      "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.95" % Test
    )

This version of `akka-persistence-cassandra` depends on Akka 2.5.13. It has been published for Scala 2.11 and 2.12.  The launcher artifact is a utility for starting an embedded Cassandra, useful for running tests. It can be removed if not needed.

To include the latest release of the Cassandra plugins for **Akka 2.4.x** into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.30"

This version of `akka-persistence-cassandra` depends on Akka 2.4.20. It has been published for Scala 2.11 and 2.12.

Those versions are compatible with Cassandra 3.0.0 or higher, and it is also compatible with Cassandra 2.1.6 or higher (versions < 2.1.6 have a static column bug) if you configure `cassandra-journal.cassandra-2x-compat=on` in your `application.conf`.

Journal plugin
--------------

### Features

- All operations required by the Akka Persistence [journal plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#journal-plugin-api) are fully supported.
- The plugin uses Cassandra in a pure log-oriented way i.e. data are only ever inserted but never updated (deletions are made on user request only).
- Writes of messages are batched to optimize throughput for `persistAsync`. See [batch writes](https://doc.akka.io/docs/akka/current/scala/persistence.html#batch-writes) for details how to configure batch sizes. The plugin was tested to work properly under high load.
- Messages written by a single persistent actor are partitioned across the cluster to achieve scalability with data volume by adding nodes.
- [Persistence Query](https://doc.akka.io/docs/akka/current/scala/persistence-query.html) support by `CassandraReadJournal`

### Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "cassandra-journal"

This will run the journal with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/master/core/src/main/resources/reference.conf):

### Caveats

- Detailed tests under failure conditions are still missing.
- Range deletion performance (i.e. `deleteMessages` up to a specified sequence number) depends on the extend of previous deletions
    - linearly increases with the number of tombstones generated by previous permanent deletions and drops to a minimum after compaction
- For versions prior to 0.80 events by tag uses Cassandra Materialized Views which are a new feature that has yet to stabilise
  Use at your own risk, see [here](https://lists.apache.org/thread.html/d81a61da48e1b872d7599df4edfa8e244d34cbd591a18539f724796f@%3Cdev.cassandra.apache.org%3E) for a recent discussion on the Cassandra dev mailing list.
  Version 0.80 and on migrated away from Materialized Views and maintain a separate table for events by tag queries.


These issues are likely to be resolved in future versions of the plugin.

### Default keyspace and table definitions

```
CREATE KEYSPACE IF NOT EXISTS akka
WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };

CREATE TABLE IF NOT EXISTS akka.messages (
  used boolean static,
  persistence_id text,
  partition_nr bigint,
  sequence_nr bigint,
  timestamp timeuuid,
  timebucket text,
  writer_uuid text,
  ser_id int,
  ser_manifest text,
  event_manifest text,
  event blob,
  meta_ser_id int,
  meta_ser_manifest text,
  meta blob,
  message blob,
  tags set<text>,
  PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
  WITH gc_grace_seconds =864000
  AND compaction = {
    'class' : 'SizeTieredCompactionStrategy',
    'enabled' : true,
    'tombstone_compaction_interval' : 86400,
    'tombstone_threshold' : 0.2,
    'unchecked_tombstone_compaction' : false,
    'bucket_high' : 1.5,
    'bucket_low' : 0.5,
    'max_threshold' : 32,
    'min_threshold' : 4,
    'min_sstable_size' : 50
    };

CREATE TABLE IF NOT EXISTS akka.tag_views (
  tag_name text,
  persistence_id text,
  sequence_nr bigint,
  timebucket bigint,
  timestamp timeuuid,
  tag_pid_sequence_nr bigint,
  writer_uuid text,
  ser_id int,
  ser_manifest text,
  event_manifest text,
  event blob,
  meta_ser_id int,
  meta_ser_manifest text,
  meta blob,
  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr))
  WITH gc_grace_seconds =864000
  AND compaction = {
    'class' : 'SizeTieredCompactionStrategy',
    'enabled' : true,
    'tombstone_compaction_interval' : 86400,
    'tombstone_threshold' : 0.2,
    'unchecked_tombstone_compaction' : false,
    'bucket_high' : 1.5,
    'bucket_low' : 0.5,
    'max_threshold' : 32,
    'min_threshold' : 4,
    'min_sstable_size' : 50
    };

CREATE TABLE IF NOT EXISTS akka.tag_write_progress(
  persistence_id text,
  tag text,
  sequence_nr bigint,
  tag_pid_sequence_nr bigint,
  offset timeuuid,
  PRIMARY KEY (persistence_id, tag));

CREATE TABLE IF NOT EXISTS akka.tag_scanning(
  persistence_id text,
  sequence_nr bigint,
  PRIMARY KEY (persistence_id));

CREATE TABLE IF NOT EXISTS akka.metadata(
  persistence_id text PRIMARY KEY,
  deleted_to bigint,
  properties map<text,text>);
```

Snapshot store plugin
---------------------

### Features

- Implements the Akka Persistence [snapshot store plugin API](https://doc.akka.io/docs/akka/current/scala/persistence.html#snapshot-store-plugin-api).

### Configuration

To activate the snapshot-store plugin, add the following line to your Akka `application.conf`:

    akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"

This will run the snapshot store with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/master/core/src/main/resources/reference.conf):

### Default keyspace and table definitions

```
CREATE KEYSPACE IF NOT EXISTS akka_snapshot
 WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };

CREATE TABLE IF NOT EXISTS akka_snapshot.snapshots (
  persistence_id text,
  sequence_nr bigint,
  timestamp bigint,
  ser_id int,
  ser_manifest text,
  snapshot_data blob,
  snapshot blob,
  meta_ser_id int,
  meta_ser_manifest text,
  meta blob,
  PRIMARY KEY (persistence_id, sequence_nr))
  WITH CLUSTERING ORDER BY (sequence_nr DESC) AND gc_grace_seconds =864000
  AND compaction = {
    'class' : 'SizeTieredCompactionStrategy',
    'enabled' : true,
    'tombstone_compaction_interval' : 86400,
    'tombstone_threshold' : 0.2,
    'unchecked_tombstone_compaction' : false,
    'bucket_high' : 1.5,
    'bucket_low' : 0.5,
    'max_threshold' : 32,
    'min_threshold' : 4,
    'min_sstable_size' : 50
    };
```

Persistence Queries
-------------------

It implements the following [Persistence Queries](https://doc.akka.io/docs/akka/current/scala/persistence-query.html):

* persistenceIds, currentPersistenceIds
* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag

Persistence Query usage example to obtain a stream with all events tagged with "someTag" with Persistence Query:

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    queries.eventsByTag("someTag", Offset.noOffset)
    
## Cleanup of tag_views table

By default the tag_views table keeps tagged events indefinitely, even when the original events have been removed. 
Depending on the volume of events this may not be suitable for production.

Before going live decide a time to live (TTL) and, if small enough, consider using the [Time Window Compaction Strategy](http://thelastpickle.com/blog/2016/12/08/TWCS-part1.html).
See `events-by-tag.time-to-live` in reference.conf for how to set this.

ScyllaDB
--------

**This project is not continuously tested against ScyllaDB.**

Some of tests are run on CircleCI in External mode only.

[![CircleCI](https://circleci.com/gh/akka/akka-persistence-cassandra.svg?style=svg)](https://circleci.com/gh/btomala/akka-persistence-cassandra)

ScyllaDB is a wire compatible replacement for Cassandra. You can run the tests
by setting `akka.persistence.cassandra.CassandraLifecycle.mode = External` and having a ScyllaDB instance accessible on localhost port 9042.
Initial testing shows that there are issues when using Scylla with persistent query which is also used for recovery. Users
with ScyllaDB experience and test environments are very welcome contributors!

For the tests and for versions before 0.80 you just run ScyllaDB with `experimental: true` to use materialized views. Versions 0.80 and onward
do not use materialized views but there are tests that do for testing the migration.

The following tests don't run in external mode as they require more control over the database:
- ReconnectSpec
- SSL tests 

Migrations
----------

### Migrations to 0.80 and later

0.80 introduces a completely different way to manage tags for events. You can skip right ahead to 0.95 without going to
0.80.

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
  done <- migrator.addTagsColumn().recover { case i: ExecutionException if i.getMessage.contains("conflicts with an existing column") => Done}
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

### Migrations from 0.54 to 0.59

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


### Migrations from 0.51 to 0.52

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

### Migrations from 0.23 to 0.50

The Persistence Query API changed slightly, see [migration guide for Akka 2.5](https://doc.akka.io/docs/akka/current/scala/project/migration-guide-2.4.x-2.5.x.html#persistence-query).

### Migrations from 0.11 to 0.12

Dispatcher configuration was changed, see [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/master/core/src/main/resources/reference.conf):

### Migrations from 0.9 to 0.10

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

### Migrations from 0.6 to 0.7

Schema changes mean that you can't upgrade from version 0.6 for Cassandra 2.x of the plugin to the 0.7 version and use existing data without schema migration. You should be able to export the data and load it to the [new table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.7/src/main/scala/akka/persistence/cassandra/journal/CassandraStatements.scala#L25).

### Migrating from 0.3.x (Akka 2.3.x)

Schema and property changes mean that you can't currently upgrade from 0.3 to 0.4 SNAPSHOT and use existing data without schema migration. You should be able to export the data and load it to the [new table definition](https://github.com/akka/akka-persistence-cassandra/blob/v0.9/src/main/scala/akka/persistence/cassandra/journal/CassandraStatements.scala).

