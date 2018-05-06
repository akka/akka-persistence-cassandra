# Releasing

From a direct clone (rather than a fork). You will need permission in sonatype to push to akka repositories.

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
