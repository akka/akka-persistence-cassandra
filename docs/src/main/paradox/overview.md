# Overview

The Akka Persistence Cassandra plugin allows for using [Apache Cassandra](https://cassandra.apache.org) as a backend for @extref:[Akka Persistence](akka:persistence.html) and @extref:[Akka Persistence Query](akka:persistence-query.html). It uses @extref:[Alpakka Cassandra](alpakka:cassandra.html) for Cassandra access which is based on the @extref:[Datastax Java Driver](java-driver:).

## Project Info

@@project-info{ projectId="core" }

## Dependencies

This plugin requires **Akka 2.6** and is built against Akka $akka.version$.

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-persistence-cassandra_$scala.binary.version$
  version=$project.version$
  symbol=AkkaVersion
  value=$akka.version$
  group1=com.typesafe.akka
  artifact1=akka-persistence
  version1=AkkaVersion
  group2=com.typesafe.akka
  artifact2=akka-persistence-query
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-cluster-tools
  version3=AkkaVersion
}

Note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Akka Persistence Cassandra’s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="core" }

## Snapshots

[bintray-badge]:  https://api.bintray.com/packages/akka/snapshots/akka-persistence-cassandra/images/download.svg
[bintray]:        https://bintray.com/akka/snapshots/akka-persistence-cassandra/_latestVersion

Snapshots are published to a snapshot repository in Sonatype after every successful build on master. Add the following to your project build definition to resolve snapshots:

sbt
:   ```scala
    resolvers += Resolver.bintrayRepo("akka", "snapshots")
    ```

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
          <id>akka-snapshots</id>
          <name>Akka Snapshots</name>
          <url>https://dl.bintray.com/akka/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://dl.bintray.com/akka/snapshots"
      }
    }
    ```

Latest published snapshot version is [![bintray-badge][]][bintray]

The [snapshot documentation](https://doc.akka.io/docs/akka-persistence-cassandra/snapshot/) is updated with every snapshot build.

## History

This [Apache Cassandra](https://cassandra.apache.org/) plugin to Akka Persistence was initiated [originally](https://github.com/krasserm/akka-persistence-cassandra) by Martin Krasser, [@krasserm](https://github.com/krasserm) in 2014.

It moved to the [Akka](https://github.com/akka/) organisation in 2016 and the first release after that move was 0.7 in January 2016.

## Contributing

Please feel free to contribute to Akka and Akka Persistence Cassandra by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).
