# Overview

The Akka Persistence Cassandra plugin allows for using [Apache Cassandra](https://cassandra.apache.org) as a backend for @extref:[Akka Persistence](akka:typed/persistence.html) and @extref:[Akka Persistence Query](akka:persistence-query.html). It uses @extref:[Alpakka Cassandra](alpakka:cassandra.html) for Cassandra access which is based on the @extref:[Datastax Java Driver](java-driver:).

## Project Info

@@project-info{ projectId="core" }

## Dependencies

This plugin requires **Akka $akka.version$** or later. See [Akka's Binary Compatibility Rules](https://doc.akka.io/docs/akka/current/common/binary-compatibility-rules.html) for details.

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-persistence-cassandra_$scala.binary.version$
  version=$project.version$
  symbol=AkkaVersion
  value=$akka.version$
  group1=com.typesafe.akka
  artifact1=akka-persistence_$scala.binary.version$
  version1=AkkaVersion
  group2=com.typesafe.akka
  artifact2=akka-persistence-query_$scala.binary.version$
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-cluster-tools_$scala.binary.version$
  version3=AkkaVersion
}

Note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Akka Persistence Cassandra’s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="core" }

To use the plugin with **Akka 2.5.x** you must use @extref:[version 0.103](apc-0.x:) or later in the 0.x series. 

## History

This [Apache Cassandra](https://cassandra.apache.org/) plugin to Akka Persistence was initiated [originally](https://github.com/krasserm/akka-persistence-cassandra) by Martin Krasser, [@krasserm](https://github.com/krasserm) in 2014.

It moved to the [Akka](https://github.com/akka/) organisation in 2016 and the first release after that move was 0.7 in January 2016.

## Contributing

Please feel free to contribute to Akka and Akka Persistence Cassandra by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka/blob/main/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).
