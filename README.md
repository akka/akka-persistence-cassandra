Cassandra Plugins for Akka Persistence
======================================

[![Join the chat at https://gitter.im/krasserm/akka-persistence-cassandra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/krasserm/akka-persistence-cassandra?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Replicated [Akka Persistence](http://doc.akka.io/docs/akka/2.3.8/scala/persistence.html) journal and snapshot store backed by [Apache Cassandra](http://cassandra.apache.org/).

[![Build Status](https://travis-ci.org/krasserm/akka-persistence-cassandra.svg?branch=master)](https://travis-ci.org/krasserm/akka-persistence-cassandra)

Dependency
----------

To include the Cassandra plugins into your `sbt` project, add the following lines to your `build.sbt` file:

    resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

    libraryDependencies += "com.github.krasserm" %% "akka-persistence-cassandra" % "0.4-SNAPSHOT"

This version of `akka-persistence-cassandra` depends on Akka Akka 2.4-RC2 and is cross-built against Scala 2.10.4 and 2.11.6. It is compatible with Cassandra 2.1.0 or higher. Versions of the Cassandra plugins that are compatible with Cassandra 1.2.x are maintained on the [cassandra-1.2](https://github.com/krasserm/akka-persistence-cassandra/tree/cassandra-1.2) branch.
   
Migrating from 0.3 (Akka 2.3)
-----------------------------

Schema and property changes mean that you can't currently upgrade from 0.3 to 0.4 SNAPSHOT and use existing data. This will be addressed in [Issue 64](https://github.com/krasserm/akka-persistence-cassandra/issues/64).

Journal plugin
--------------

### Features

- All operations required by the Akka Persistence [journal plugin API](http://doc.akka.io/docs/akka/2.3.9/scala/persistence.html#journal-plugin-api) are fully supported.
- The plugin uses Cassandra in a pure log-oriented way i.e. data are only ever inserted but never updated (deletions are made on user request only or by persistent channels, see also [Caveats](#caveats)).
- Writes of messages and confirmations are batched to optimize throughput. See [batch writes](http://doc.akka.io/docs/akka/2.3.9/scala/persistence.html#batch-writes) for details how to configure batch sizes. The plugin was tested to work properly under high load.
- Messages written by a single processor are partitioned across the cluster to achieve scalability with data volume by adding nodes.

### Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "cassandra-journal"

This will run the journal with its default settings. The default settings can be changed with the following configuration keys:

- `cassandra-journal.contact-points`. A comma-separated list of contact points in a Cassandra cluster. Default value is `[127.0.0.1]`. Host:Port pairs are also supported. In that case the port parameter will be ignored.
- `cassandra-journal.port`. Port to use to connect to the Cassandra host. Default value is `9042`. Will be ignored if the contact point list is defined by host:port pairs.
- `cassandra-journal.keyspace`. Name of the keyspace to be used by the plugin. Default value is `akka`.
- `cassandra-journal.keyspace-autocreate`. Boolean parameter indicating whether the keyspace should be automatically created if it doesn't exist. Default value is `true`.
- `cassandra-journal.keyspace-autocreate-retries`. Int parameter which defines a number of retries before giving up on automatic schema creation. Default value is `1`.
- `cassandra-journal.table`. Name of the table to be used by the plugin. If the table doesn't exist it is automatically created. Default value is `messages`.
- `cassandra-journal.replication-strategy`. Replication strategy to use. SimpleStrategy or NetworkTopologyStrategy
- `cassandra-journal.replication-factor`. Replication factor to use when a keyspace is created by the plugin. Default value is `1`.
- `cassandra-journal.data-center-replication-factors`. Replication factor list for data centers, e.g. ["dc1:3", "dc2:2"]. Is only used when replication-strategy is NetworkTopologyStrategy.
- `cassandra-journal."max-message-batch-size"`. Maximum number of messages that will be batched when using `persistAsync`. Also used as the max batch size for deletes.
- `cassandra-journal.delete-retries`. Deletes are achieved using a metadata entry and then the actual messages are deleted asynchronously. Number of retries before giving up. Default value is 3. 
- `cassandra-journal.target-partition-size`. Target number of messages per cassandra partition. Default value is 500000. Will only go above the target if you use persistAll and persistAllAsync **Do not change this setting after table creation** (not checked yet).
- `cassandra-journal.max-result-size`. Maximum number of entries returned per query. Queries are executed recursively, if needed, to achieve recovery goals. Default value is 50001.
- `cassandra-journal.write-consistency`. Write consistency level. Default value is `QUORUM`.
- `cassandra-journal.read-consistency`. Read consistency level. Default value is `QUORUM`.

The default read and write consistency levels ensure that processors can read their own writes. During normal operation, processors only write to the journal, reads occur only during recovery.

To connect to the Cassandra hosts with credentials, add the following lines:

- `cassandra-journal.authentication.username`. The username to use to login to Cassandra hosts. No authentication is set as default.
- `cassandra-journal.authentication.password`. The password corresponding to username. No authentication is set as default.

To connect to the Cassandra host with SSL enabled, add the following configuration. For detailed instructions, please refer to the [DataStax Cassandra chapter about SSL Encryption](http://docs.datastax.com/en/cassandra/2.0/cassandra/security/secureSslEncryptionTOC.html).

- `cassandra-journal.ssl.truststore.path`. Path to the JKS Truststore file.
- `cassandra-journal.ssl.truststore.password`. Password to unlock the JKS Truststore.
- `cassandra-journal.ssl.keystore.path`. Path to the JKS Keystore file.
- `cassandra-journal.ssl.keystore.password`. Password to unlock JKS Truststore and access the private key (both must use the same password).

To limit the Cassandra hosts this plugin connects with to a specific datacenter, use the following setting:

- `cassandra-journal.local-datacenter`.  The id for the local datacenter of the Cassandra hosts that this module should connect to.  By default, this property is not set resulting in Datastax's standard round robin policy being used.

### Caveats

- Detailed tests under failure conditions are still missing.
- Range deletion performance (i.e. `deleteMessages` up to a specified sequence number) depends on the extend of previous deletions
    - linearly increases with the number of tombstones generated by previous permanent deletions and drops to a minimum after compaction
    - linearly increases with the number of plugin-level deletion markers generated by previous logical deletions (recommended: always use permanent range deletions)

These issues are likely to be resolved in future versions of the plugin.

Snapshot store plugin
---------------------

### Features

- Implements its own handler of the (internal) Akka Persistence snapshot protocol, making snapshot IO fully asynchronous (i.e. does not implement the Akka Persistence [snapshot store plugin API](http://doc.akka.io/docs/akka/2.3.9/scala/persistence.html#snapshot-store-plugin-api) directly).

### Configuration

To activate the snapshot-store plugin, add the following line to your Akka `application.conf`:

    akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"

This will run the snapshot store with its default settings. The default settings can be changed with the following configuration keys:

- `cassandra-snapshot-store.contact-points`. A comma-separated list of contact points in a Cassandra cluster. Default value is `[127.0.0.1]`. Host:Port pairs are also supported. In that case the port parameter will be ignored.
- `cassandra-snapshot-store.port`. Port to use to connect to the Cassandra host. Default value is `9042`. Will be ignored if the contact point list is defined by host:port pairs.
- `cassandra-snapshot-store.keyspace`. Name of the keyspace to be used by the plugin. Default value is `akka_snapshot`.
- `cassandra-snapshot-store.keyspace-autocreate`. Boolean parameter indicating whether the keyspace should be automatically created if it doesn't exist. Default value is `true`.
- `cassandra-snapshot-store.keyspace-autocreate-retries`. Int parameter which defines a number of retries before giving up on automatic schema creation. Default value is `1`.
- `cassandra-snapshot-store.table`. Name of the table to be used by the plugin. If the table doesn't exist it is automatically created. Default value is `snapshots`.
- `cassandra-snapshot-store.replication-strategy`. Replication strategy to use. SimpleStrategy or NetworkTopologyStrategy
- `cassandra-snapshot-store.replication-factor`. Replication factor to use when a keyspace is created by the plugin. Default value is `1`.
- `cassandra-snapshot-store.data-center-replication-factors`. Replication factor list for data centers, e.g. ["dc1:3", "dc2:2"]. Is only used when replication-strategy is NetworkTopologyStrategy.
- `cassandra-snapshot-store.max-metadata-result-size`. Maximum number of snapshot metadata to load per recursion (when trying to find a snapshot that matches specified selection criteria). Default value is `10`. Only increase this value when selection criteria frequently select snapshots that are much older than the most recent snapshot i.e. if there are much more than 10 snapshots between the most recent one and selected one. This setting is only for increasing load efficiency of snapshots.
- `cassandra-snapshot-store.write-consistency`. Write consistency level. Default value is `ONE`.
- `cassandra-snapshot-store.read-consistency`. Read consistency level. Default value is `ONE`.

To connect to the Cassandra hosts with credentials, add the following lines:

- `cassandra-snapshot-store.authentication.username`. The username to use to login to Cassandra hosts. No authentication is set as default.
- `cassandra-snapshot-store.authentication.password`. The password corresponding to username. No authentication is set as default.

To connect to the Cassandra host with SSL enabled, add the following configuration. For detailed instructions, please refer to the [DataStax Cassandra chapter about SSL Encryption](http://docs.datastax.com/en/cassandra/2.0/cassandra/security/secureSslEncryptionTOC.html).

- `cassandra-snapshot-store.ssl.truststore.path`. Path to the JKS Truststore file.
- `cassandra-snapshot-store.ssl.truststore.password`. Password to unlock the JKS Truststore.
- `cassandra-snapshot-store.ssl.keystore.path`. Path to the JKS Keystore file.
- `cassandra-snapshot-store.ssl.keystore.password`. Password to unlock JKS Truststore and access the private key (both must use the same password).


To limit the Cassandra hosts this plugin connects with to a specific datacenter, use the following setting:

- `cassandra-snapshot-store.local-datacenter`.  The id for the local datacenter of the Cassandra hosts that this module should connect to.  By default, this property is not set resulting in Datastax's standard round robin policy being used.
