# Overview

The Akka Persistence Cassandra plugin allows for using [Apache Cassandra](https://cassandra.apache.org) as a backend for [Akka Persistence](https://doc.akka.io/docs/akka/current/persistence.html) and [Akka Persistence Query](https://doc.akka.io/docs/akka/current/persistence-query.html).

## Project Info

@@project-info{ projectId="core" }

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-persistence-cassandra_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka 2.5.x and note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Akka Persistence Cassandra’s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="core" }

## History

This [Apache Cassandra](https://cassandra.apache.org/) plugin to Akka Persistence was initiated [originally](https://github.com/krasserm/akka-persistence-cassandra) by Martin Krasser, [@krasserm](https://github.com/krasserm) in 2014.

It moved to the [Akka](https://github.com/akka/) organisation in 2017 and the first release after that move was 0.80 in January 2018.


## Contributing

Please feel free to contribute to Akka and Akka Persistence Couchbase Documentation by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/akka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://github.com/akka/akka/blob/master/CODE_OF_CONDUCT.md).
