scalaVersion := "2.11.1"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.1"

scalacOptions += "-language:implicitConversions"

scalacOptions += "-deprecation"

scalacOptions += "-Yinline-warnings"

unmanagedJars in Compile <<= baseDirectory map { base => (base ** "*.jar").classpath }

// there is a bug in SBT that does not allow compiler plugins to have dependencies
addCommandAlias("embed", ";clean ;legolifter/run ;project lego-core ;set scalacOptions ++= Seq(s\"-Xplugin:${System.getProperty(\"user.home\")}/.ivy2/local/ch.epfl.data/autolifter_2.11/0.1-SNAPSHOT/jars/autolifter_2.11.jar:${System.getProperty(\"user.home\")}/.ivy2/local/ch.epfl.lamp/yy-core_2.11/0.1-SNAPSHOT/jars/yy-core_2.11.jar:${System.getProperty(\"user.home\")}/.ivy2/local/ch.epfl.lamp/yin-yang_2.11/0.1-SNAPSHOT/jars/yin-yang_2.11.jar\"); compile; reload; project root; clean")
