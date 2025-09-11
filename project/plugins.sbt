resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("com.github.sbt" % "sbt-multi-jvm" % "0.6.0")
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.1.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")

// for dependency analysis
addDependencyTreePlugin

// Documentation
addSbtPlugin("io.akka" % "sbt-paradox-akka" % "25.10.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.4")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0")

// For example
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
