scalaVersion := "2.11.1"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.1"

scalacOptions += "-language:postfixOps"

scalacOptions += "-language:implicitConversions"

scalacOptions += "-deprecation"

scalacOptions += "-optimise"

scalacOptions += "-Yinline-warnings"

unmanagedJars in Compile <<= baseDirectory map { base => (base ** "*.jar").classpath }
