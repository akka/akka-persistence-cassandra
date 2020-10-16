# Cassandra server support

There are several alternative Cassandra server implementations. This project is tested with 
[Apache Cassandra](https://cassandra.apache.org) and
[DataStax Enterprise (DSE)](https://www.datastax.com/resources/datasheet/datastax-enterprise).

[DataStax Astra](https://www.datastax.com/products/datastax-astra) is built on Apache Cassandra and should therefore also work without
surprises even though there is no continuous integration tests for it in this project. See @ref:[DataStax Astra configuration](astra.md).

[Lightbend support](https://www.lightbend.com/lightbend-subscription) only covers usage with Apache Cassandra, DSE and DataStax Astra. 

Several other alternatives are listed below. Be aware of that this project is not continuously tested against those
and currently not covered by Lightbend support. 

* @ref:[CosmosDB](cosmosdb.md)
* @ref:[DataStax Astra](astra.md)
* @ref:[ScyllaDB](scylladb.md)

## More details

@@toc { depth=1 }

@@@ index

* [Amazon Keyspaces](keyspaces.md)
* [CosmosDB](cosmosdb.md)
* [DataStax Astra](astra.md)
* [ScyllaDB](scylladb.md)

@@@
