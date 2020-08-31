addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.6")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-osgi" % "0.9.4")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.10")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

// Documentation
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-project-info" % "1.1.2")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.35")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.1")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.0")
// depend directly on the patched version see https://github.com/akka/alpakka/issues/1388
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
// patched version of sbt-dependency-graph and sbt-site
resolvers += Resolver.bintrayIvyRepo("2m", "sbt-plugins")
