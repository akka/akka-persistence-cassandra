# Snapshots

### Features

- Implements the Akka Persistence @extref:[snapshot store plugin API](akka:scala/persistence.html#snapshot-store-plugin-api).

### Configuration

To activate the snapshot-store plugin, add the following line to your Akka `application.conf`:

    akka.persistence.snapshot-store.plugin = "cassandra-plugin.snapshot"

This will run the snapshot store with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/master/core/src/main/resources/reference.conf):


### Keyspace and table definitions

The default keyspace used by the plugin is called `akka_snapshot`. Auto creation of the keyspace and tables
is included as a development convenience and should never be used in production. Cassandra does not handle
concurrent schema migrations well and if every Akka node tries to create the schema at the same time you'll
get column id mismatch errors in Cassandra. The keyspace should be created with the
NetworkTopology replication strategy with a replication factor of at least 3:

```
CREATE KEYSPACE IF NOT EXISTS akka_snapshot WITH replication = {'class': 'NetworkTopologyStrategy', '<your_dc_name>' : 3 }; 
```

For local testing, and the default if you enable `cassandra-plugin.snapshot.keyspace-autocreate` you can use the following:

@@snip [snapshot-keyspace](/target/snapshot-keyspace.txt) { #snapshot-keyspace } 

A single table is required. This needs to be created before starting your application.
For local testing you can enable `cassnadra-plugin.snapshot.table-autocreate`

@@snip [snapshot-tables](/target/snapshot-tables.txt) { #snapshot-tables} 

#### Snapshot settings

Under `cassandra-plugin.snapshot`:

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #snapshot }

##### Cassandra driver overrides

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #profile }

##### Shared settings for all parts of the plugin

The following settings are shared by the `journal`, `query`, and `snapshot` parts of the plugin and are under
`cassandra-plugin`: 

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #shared }




