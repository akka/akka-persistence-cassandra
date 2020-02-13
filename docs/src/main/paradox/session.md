# Apache Cassandra

@@@ note { title="Cassandra"}

Apache Cassandra is a free and open-source, distributed, wide column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure. Cassandra offers robust support for clusters spanning multiple datacenters, with asynchronous masterless replication allowing low latency operations for all clients.

-- [Wikipedia](https://en.wikipedia.org/wiki/Apache_Cassandra)

@@@

Alpakka Cassandra offers an @extref:[Akka Streams](akka:/streams/index.html) API on top of a wrapper of the `CqlSession` from the @extref:[Datastax Java Driver](java-driver:) version 4.0+. The @extref:[driver configuration](java-driver:/manual/core/configuration/#quick-overview) is provided in the same config format as Akka uses.

@@project-info{ projectId="core" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-cassandra_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="cassandra" }


## Sessions

Cassandra is accessed through @apidoc[CassandraSession]s which are managed by the @apidoc[CassandraSessionRegistry] Akka extension. This way a session is shared across all usages within the actor system and properly shut down after the actor system is shut down. 

@scala[The `CassandraSession` is provided to the stream factory methods as `implicit` parameter.]

Scala
: @@snip [snip](/session/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #init-session }

Java
: @@snip [snip](/session/src/test/java/docs/javadsl/CassandraSourceTest.java) { #init-session }

See @ref[custom session creation](#custom-session-creation) below for tweaking this.


## Reading from Cassandra

@apidoc[CassandraSource] provides factory methods to get Akka Streams Sources from CQL queries and from statements.

Dynamic parameters can be provided to the CQL as variable arguments.

Scala
: @@snip [snip](/session/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #cql }

Java
: @@snip [snip](/session/src/test/java/docs/javadsl/CassandraSourceTest.java) { #cql }


If the statement requires specific settings, 

Scala
: @@snip [snip](/session/src/test/scala/docs/scaladsl/CassandraSourceSpec.scala) { #statement }

Java
: @@snip [snip](/session/src/test/java/docs/javadsl/CassandraSourceTest.java) { #statement }


Here we used a basic sink to complete the stream by collecting all of the stream elements to a collection. The power of streams comes from building larger data pipelines which leverage backpressure to ensure efficient flow control. Feel free to edit the example code and build @extref:[more advanced stream topologies](akka:stream/stream-introduction.html).


## Updating Cassandra

@apidoc[CassandraFlow] provides factory methods to get Akka Streams flows to run CQL updated statements. Alpakka Cassandra creates a `PreparedStatement` and for every stream element the `statementBinder` function binds the CQL placeholders to data.

The incoming elements are emitted unchanged for further processing. 

Scala
: @@snip [snip](/session/src/test/scala/docs/scaladsl/CassandraFlowSpec.scala) { #prepared }

Java
: @@snip [snip](/session/src/test/java/docs/javadsl/CassandraFlowTest.java) { #prepared }


Alpakka Cassandra flows offer **"With Context"**-support which integrates nicely with some other Alpakka connectors.

Scala
: @@snip [snip](/session/src/test/scala/docs/scaladsl/CassandraFlowSpec.scala) { #withContext }

Java
: @@snip [snip](/session/src/test/java/docs/javadsl/CassandraFlowTest.java) { #withContext }


## Custom Session creation

# TODO

See @apidoc[CqlSessionProvider]

application.conf
: @@snip [snip](/session/src/main/resources/reference.conf)
