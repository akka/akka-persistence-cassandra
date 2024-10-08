# Testing

There are a number of options for testing persistent actors when using the Akka Persistence Cassandra plugin.
The two main methods are:

* Testing using the inmem journal as shown in the [Akka docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence-testing.html).
* Testing against a real Cassandra instance.

For testing against Cassandra you can:

* Use an existing cluster in your environment.
* Install a local Cassandra cluster installed with your preferred method
* Set up one locally with [Cassandra Cluster Manager](https://github.com/riptano/ccm)

Then there are options with tighter unit test framework integration:

* [Cassandra unit](https://github.com/jsevellec/cassandra-unit)
* [Test Containers](https://www.testcontainers.org/modules/databases/cassandra/)

For testing it can be convenient to enable automatic creation of keyspace and tables with configuration:

    akka.persistence.cassandra {
      journal.keyspace-autocreate = on
      journal.tables-autocreate = on
      snapshot.keyspace-autocreate = on
      snapshot.tables-autocreate = on
    }


Another recommended configuration is to not run test in parallel to avoid contention around manipulating the cassandra keyspace. Using sbt, you are able to control the `parallelExecution` as described [here](https://www.scala-sbt.org/1.x/docs/Testing.html).
