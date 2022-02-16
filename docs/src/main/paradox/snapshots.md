# Snapshots

## Features

- Implements the Akka Persistence @extref:[snapshot store plugin API](akka:persistence-journals.html#snapshot-store-plugin-api).

## Schema

The keyspace and tables needs to be created before using the plugin. 
  
@@@ warning

Auto creation of the keyspace and tables
is included as a development convenience and should never be used in production. Cassandra does not handle
concurrent schema migrations well and if every Akka node tries to create the schema at the same time you'll
get column id mismatch errors in Cassandra.

@@@

The default keyspace used by the plugin is `akka_snapshot`, it should be created with the
NetworkTopology replication strategy with a replication factor of at least 3:

```
CREATE KEYSPACE IF NOT EXISTS akka_snapshot WITH replication = {'class': 'NetworkTopologyStrategy', '<your_dc_name>' : 3 }; 
```

For local testing, and the default if you enable `akka.persistence.cassandra.snapshot.keyspace-autocreate` you can use the following:

@@snip [snapshot-keyspace](/target/snapshot-keyspace.txt) { #snapshot-keyspace } 

A single table is required. This needs to be created before starting your application.
For local testing you can enable `akka.persistence.cassandra.snapshot.tables-autocreate`.
The default table definitions look like this:

@@snip [snapshot-tables](/target/snapshot-tables.txt) { #snapshot-tables}

### Consistency

By default, the snapshot store uses `ONE` for all reads and writes, since snapshots
should only be used as an optimization to reduce number of replayed events.
If a recovery doesn't see the latest snapshot it will just start from an older snapshot
and replay events from there. Be careful to not delete events too eagerly after storing
snapshots since the deletes may be visible before the snapshot is visible. Keep a few
snapshots and corresponding events before deleting older events and snapshots.

The consistency level for snapshots can be changed with:

```
datastax-java-driver.profiles {
  akka-persistence-cassandra-snapshot-profile {
    basic.request.consistency = QUORUM
  }
}
```

## Configuration

To activate the snapshot-store plugin, add the following line to your Akka `application.conf`:

    akka.persistence.snapshot-store.plugin = "akka.persistence.cassandra.snapshot"

This will run the snapshot store with its default settings. The default settings can be changed with the configuration
properties defined in @ref:[reference.conf](configuration.md#default-configuration). Journal configuration is under 
`akka.persistence.cassandra.snapshot`.

## Limitations

The snapshot is stored in a single row so the maximum size of a serialized snapshot is the Cassandra configured
[`max_mutation_size_in_kb`](https://cassandra.apache.org/doc/latest/faq/index.html#can-large-blob) which is 16MB by default.

## Delete all snapshots

The @apidoc[akka.persistence.cassandra.cleanup.Cleanup] tool can be used for deleting all events and/or snapshots
given list of `persistenceIds` without using persistent actors. It's important that the actors with corresponding
`persistenceId` are not running at the same time as using the tool. See @ref[Database Cleanup](./cleanup.md) for more details.
