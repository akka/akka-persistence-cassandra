# Releasing

From a direct clone (rather than a fork). You will need permission in sonatype to push to akka repositories.

* run `core/test:runMain akka.persistence.cassandra.PrintCreateStatements` if keyspace/table statements or config have changed,
  paste into README.md
* commit and push a new version number in the README.md
* sbt -Dpublish.maven.central=true
  * clean
  * release skip-tests
* close the staging repo in sonatype (https://oss.sonatype.org/#welcome)
* verify the contents of the staging
* release the staging repo in sonatype
* push to origin including tags
* WhiteSource
  * update the 'akka-persistence-cassandra-xx-stable' project name in [WhiteSource](https://saas.whitesourcesoftware.com)
  * checkout the released version, e.g. v0.80
  * `sbt whitesourceUpdate`
* Ask someone in the Akka team to update the [Lightbend Platform supported modules list](https://developer.lightbend.com/docs/lightbend-platform/2.0/supported-modules/) (requires Lightbend private GitHub permission). Only for stable releases, not milestones/RCs.
