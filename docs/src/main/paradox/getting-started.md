# Getting started

### Latest release

# TODO be clear about versions

To include the latest release of the Cassandra plugins for **Akka 2.5.x** into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += Seq(
      "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.99",
      "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.99" % Test
    )

This version of `akka-persistence-cassandra` depends on Akka 2.5.23. It has been published for Scala 2.12 and 2.13.  The launcher artifact is a utility for starting an embedded Cassandra, useful for running tests. It can be removed if not needed.

To include the latest release of the Cassandra plugins for **Akka 2.5.x** compiled for Scala 2.11 into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += Seq(
      "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.98",
      "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.98" % Test
    )

This version of `akka-persistence-cassandra` depends on Akka 2.5.13. It has been published for Scala 2.11 and 2.12.  The launcher artifact is a utility for starting an embedded Cassandra, useful for running tests. It can be removed if not needed.

To include the latest release of the Cassandra plugins for **Akka 2.4.x** into your `sbt` project, add the following lines to your `build.sbt` file:

    libraryDependencies += "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.30"

This version of `akka-persistence-cassandra` depends on Akka 2.4.20. It has been published for Scala 2.11 and 2.12.

Those versions are compatible with Cassandra 3.0.0 or higher, and it is also compatible with Cassandra 2.1.6 or higher (versions < 2.1.6 have a static column bug) if you configure `cassandra-journal.cassandra-2x-compat=on` in your `application.conf`.
