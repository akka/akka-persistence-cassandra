# Testing

There are numbers options for testing persistent actors when using the APC plugin.
The two main methods are:

* Testing using the inmem or Leveldb journals as shown in the [Akka docs](https://doc.akka.io/docs/akka/current/typed/persistence-testing.html).
* Testing against a real Cassandra instance.

For testing against Cassandra you can:

* Use an existing cluster in your environment.
* Install a local Cassandra cluster installed with your preferred method
* Set up one locally with [Cassandra Cluster Manager](https://github.com/riptano/ccm)

Then there are options with tighter unit test framework integration:

* [Cassandra unit](https://github.com/jsevellec/cassandra-unit)
* [Test Containers](https://www.testcontainers.org/modules/databases/cassandra/)




