# Overview

The Akka Persistence Cassandra plugin allows for using [Apache Cassandra](https://cassandra.apache.org) as a backend for @extref:[Akka Persistence](akka:persistence.html) and @extref:[Akka Persistence Query](akka:persistence-query.html).

## Project Info

@@project-info{ projectId="core" }

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-persistence-cassandra_$scala.binary.version$
  version=$project.version$
  group2=com.typesafe.akka
  artifact2=akka-persistence-cassandra-launcher_$scala.binary.version$
  version2=$project.version$
  scope2=Test
}

The launcher artifact is a utility for starting an embedded Cassandra, useful for running tests. It can be removed if not needed.

Those versions are compatible with Cassandra 3.0.0 or higher, and it is also compatible with Cassandra 2.1.6 or higher (versions < 2.1.6 have a static column bug) if you configure `cassandra-journal.cassandra-2x-compat=on` in your `application.conf`.

This plugin depends on Akka 2.5.x and note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Akka Persistence Cassandra’s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="core" }

## Library snapshots

[sonatype-badge]: https://img.shields.io/nexus/s/https/oss.sonatype.org/com.typesafe.akka/akka-persistence-cassandra_2.12.svg?label=latest%20snapshot
[sonatype]:       https://oss.sonatype.org/content/repositories/snapshots/com/typesafe/akka/akka-persistence-cassandra_2.12/

Snapshots are published to a snapshot repository in Sonatype after every successful build on master. Add the following to your project build definition to resolve snapshots:

sbt
:   ```scala
    resolvers += Resolver.sonatypeRepo("snapshots")
    ```

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
          <id>sonatype-snapshots</id>
          <name>Sonatype Snapshots</name>
          <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://oss.sonatype.org/content/repositories/snapshots"
      }
    }
    ```

Latest published snapshot version is [![sonatype-badge][]][sonatype]

The [snapshot documentation](https://doc.akka.io/docs/akka-persistence-cassandra/snapshot/) is updated with every snapshot build.

## History

This [Apache Cassandra](https://cassandra.apache.org/) plugin to Akka Persistence was initiated [originally](https://github.com/krasserm/akka-persistence-cassandra) by Martin Krasser, [@krasserm](https://github.com/krasserm) in 2014.

It moved to the [Akka](https://github.com/akka/) organisation in 2016 and the first release after that move was 0.7 in January 2016.

## Contributing

Please feel free to contribute to Akka and Akka Persistence Couchbase Documentation by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka-persistence-cassandra/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://github.com/akka/akka/blob/master/CODE_OF_CONDUCT.md).
