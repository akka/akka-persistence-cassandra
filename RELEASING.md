# Releasing

From a direct clone (rather than a fork). You will need permission in sonatype to push to akka repositories.

* run `core/test:runMain akka.persistence.cassandra.PrintCreateStatements` if keyspace/table statements or config have changed,
  paste into README.md
* commit and push a new version number in the README.md
* make sure to use JDK 8
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
* Ask someone in the Akka team to update the [Lightbend Platform "Library build dependencies" page](https://developer.lightbend.com/docs/lightbend-platform/introduction/getting-help/build-dependencies.html#_akka_persistence_cassandra) (requires Lightbend private GitHub permission). Only for stable releases, not milestones/RCs.
* Make a release announcement 
* When the tag is pushed the documentation will be built automatically, but you need to update the links: 
```
ssh akkarepo@gustav.akka.io
cd www
ln -nsf $VERSION$ api/akka-persistence-cassandra/current
ln -nsf $VERSION$ docs/akka-persistence-cassandra/current
git add docs/akka-persistence-cassandra/current api/akka-persistence-cassandra/current api/akka-persistence-cassandra/$VERSION$ docs/akka-persistence-cassandra/$VERSION$
git commit -m "akka persistence cassandra $VERSION$
```
