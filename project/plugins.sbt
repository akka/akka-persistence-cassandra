addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.5")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.49")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")

// For example
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.7.0")
