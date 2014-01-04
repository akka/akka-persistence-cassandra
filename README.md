Cassandra Journal for Akka Persistence
======================================

A replicated [Akka Persistence](http://doc.akka.io/docs/akka/2.3-M2/scala/persistence.html) journal backed by [Apache Cassandra](http://cassandra.apache.org/).

Prerequisites
-------------

<table border="0">
  <tr>
    <td>Akka version: </td>
    <td>2.3-M2 or higher</td>
  </tr>
  <tr>
    <td>Cassandra version: </td>
    <td>2.0.3 or higher</td>
  </tr>
</table>

Installation
------------

Build and install the journal plugin to your local Ivy cache with `sbt publishLocal` (requires sbt 0.13). It can then be included as dependency:

    libraryDependencies += "com.github.krasserm" %% "akka-persistence-cassandra" % "0.1-SNAPSHOT"

Configuration
-------------

To activate the Cassandra journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "cassandra-journal"

This will run the journal with its default settings. The default settings can be changed with the following configuration keys:

- `cassandra-journal.contact-points`. A comma-separated list of contact points in a Cassandra cluster. Default value is `[127.0.0.1]`.
- `cassandra-journal.keyspace`. Name of the keyspace to be used by the plugin. If the keyspace doesn't exist it is automatically created. Default value is `akka`.
- `cassandra-journal.table`. Name of the table to be used by the plugin. If the table doesn't exist it is automatically created. Default value is `messages`.
- `cassandra-journal.replication-factor`. Replication factor to use when a keyspace is created by the plugin. Default value is `1`.
- `cassandra-journal.write-consistency`. Write consistency level. Default value is `QUORUM`.
- `cassandra-journal.read-consistency`. Read consistency level. Default value is `QUORUM`.

The default read and write consistency levels ensure that processors can read their own writes. Applications may also choose to configure

- `cassandra-journal.write-consistency = "ONE"`
- `cassandra-journal.read-consistency = "ALL"`

which increases write throughput but lowers replay throughput and availability during recovery. During normal operation, processors only write to the journal, reads occur only during recovery.

Status
------

- All operations required by the Akka Persistence [journal plugin API](http://doc.akka.io/docs/akka/2.3-M2/scala/persistence.html#journal-plugin-api) are supported.
- Row splitting per processor is implemented so that a large number of messages per processor can be stored.
- Message writes are batched to optimize throughput. When using channels, confirmation writes are not batched yet.
- Persistent channel recovery is not optimized yet. For details and possible optimizations details see [issue 4](https://github.com/krasserm/akka-persistence-cassandra/issues/4).
- The plugin was tested to properly work under high load. Detailed tests under failure conditions are still missing.
