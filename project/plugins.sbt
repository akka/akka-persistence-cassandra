addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.5")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "5.0.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

// Documentation
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.49")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0")

// For example
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")
