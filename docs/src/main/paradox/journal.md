# Journal

## Features

- All operations required by the Akka Persistence @extref:[journal plugin API](akka:persistence-journals.html#journal-plugin-api) are fully supported.
- The plugin uses Cassandra in a pure log-oriented way i.e. data is only ever inserted but never updated (deletions are made on user request only).
- Writes of messages are batched to optimize throughput for `persistAsync`. See @extref:[batch writes](akka:persistence.html#batch-writes) for details how to configure batch sizes. The plugin was tested to work properly under high load.
- Messages written by a single persistent actor are partitioned across the cluster to achieve scalability with data volume by adding nodes.
- @extref:[Persistence Query](akka:persistence-query.html) support by `CassandraReadJournal`

## Schema 

The keyspace and tables needs to be created before using the plugin. 
  
@@@ warning

Auto creation of the keyspace and tables
is included as a development convenience and should never be used in production. Cassandra does not handle
concurrent schema migrations well and if every Akka node tries to create the schema at the same time you'll
get column id mismatch errors in Cassandra.

@@@

The default keyspace used by the plugin is `akka`, it should be created with the
NetworkTopology replication strategy with a replication factor of at least 3:

```
CREATE KEYSPACE IF NOT EXISTS akka WITH replication = {'class': 'NetworkTopologyStrategy', '<your_dc_name>' : 3 }; 
```

For local testing, and the default if you enable `akka.persistence.cassandra.journal.keyspace-autocreate` you can use the following:

@@snip [journal-schema](/target/journal-keyspace.txt) { #journal-keyspace } 

There are multiple tables required. These need to be created before starting your application.
For local testing you can enable `cassandra-plugin.journal.table-autocreate`. The default table definitions look like this:

@@snip [journal-tables](/target/journal-tables.txt) { #journal-tables } 

### Messages table

Descriptions of the important columns in the messages table:

| Column            | Description                                                                                                                                |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| persistence_id    | The persistence id                                                                                                                         |
| partition_nr      | Artificial partition key to ensure partitions do not grow too large                                                                        |
| sequence_nr       | Sequence number of the event                                                                                                               |
| timestamp         | A [type 2 UUID a.k.a TimeUUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) for ordering events in the events by tag query |
| timebucket        | The time bucket to partition the events by tag query. Only in this table as events by tag used to use a materialized view                  |
| writer_uuid       | A UUID for the actor system that wrote the event. Used to detect multiple writers for the same persistence id                              |
| ser_id            | The serialization id of the user payload                                                                                                   |
| ser_manifest      | The serialization manifest of the user payload                                                                                             |
| event_manifest    | The manifest used by event adapters                                                                                                        |
| event             | The serialized user payload                                                                                                                |

Old columns, no longer needed but may be in your schema if you have used older versions of the plugin and migrated. See
@ref[the  migration guide](./migrations.md) for when these have been removed.

| Column  | Description                                                                                                                             |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------|
| used    | A static column to record that the artificial partition has been used to detected that all events in a partition have been deleted      |
| message | Pre 0.6 serialized the PersistentRepr (an internal Akka type) into this column. Newer versions use event and serialize the user payload |
|         |                                                                                                                                         |

## Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "akka.persistence.cassandra.journal"

This will run the journal with its default settings. The default settings can be changed with the configuration properties defined in
@ref:[reference.conf](configuration.md#default-configuration). Journal configuration is under `akka.persistence.cassandra.journal`.

All Cassandra driver settings are via its @extref:[standard profile mechanism](java-driver:manual/core/configuration/).

One important setting is to configure the database driver to retry the initial connection:

`datastax-java-driver.advanced.reconnect-on-init = true`

It is not enabled automatically as it is in the driver's reference.conf and is not overridable in a profile.

### Target partition size

The messages table that stores the events is partitioned by `(persistence_id, partition_nr)`. The `partition_nr` is an
artificial partition key to ensure that the Cassandra partition does not get too large if there are a lot of events for
a single `persistence_id`.

`akka.persistence.cassandra.journal.target-partition-size` controls the number of events that the journal tries to put
in each Cassandra partition. It is a target as `persistAll` calls will have all the events in the same partition
even if it will exceed the target partition size to ensure atomicity.

It is not possible to change the value once you have data so consider if the default of 500000 is right for your
application before deploying to production. Multiply the value by your expected serialized event size to roughly work
out how large the Cassandra partition will grow to. See [wide partitions in
Cassandra](https://thelastpickle.com/blog/2019/01/11/wide-partitions-cassandra-3-11.html) for a summary of how large a
partition should be depending on the version of Cassandra you are using. 

### Consistency

By default the journal uses `QUORUM` for all reads and writes.
For setups with multiple datacenters this can set to `LOCAL_QUORUM` to
avoid cross DC latency for writes and reads.

The risk of using `LOCAL_QUORUM` is that in the event of a datacenter outage events that have been confirmed
and any side effects run may not have be replicated to the other datacenters.
If a persistent actor for which this has happened is started in another datacenter it may not see the latest event
if it wasn't replicated.
If the Cassandra data in the datacenter with the outage is recovered then the event that was not replicated will 
eventually be replicated to all datacenters resulting in a duplicate sequence number.
With the default [`replay-filter`](https://doc.akka.io/docs/akka/current/typed/persistence.html#replay-filter) the
duplicate event from the original datacenter will is discarded in subsequent replays of the persistent actor.

Using `QUORUM` for multi datacenter setups increases latency and decreased availability as to reach `QUORUM` nodes in
other datacenters need to respond. During a datacenter outage or a cross datacenter network partition this won't be
possible resulting in failed reads and writes.

Using a consistency level other than `QUORUM` or `LOCAL_QUORUM` is highly discouraged.

```
datastax-java-driver.profiles {
  akka-persistence-cassandra-profile {
    basic.request.consistency = QUORUM
  }
}
```

## Delete all events

The @apidoc[akka.persistence.cassandra.cleanup.Cleanup] tool can be used for deleting all events and/or snapshots
given list of `persistenceIds` without using persistent actors. It's important that the actors with corresponding
`persistenceId` are not running at the same time as using the tool. See @ref[Database Cleanup](./cleanup.md) for more details.
