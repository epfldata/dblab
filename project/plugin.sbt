addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.3.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.1.0")

resolvers += Resolver.sonatypeRepo("snapshots")

addSbtPlugin("ch.epfl.data" % "sc-purgatory-plugin" % "0.1.3-SNAPSHOT")

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.4")
