import java.io.File
import sbt._
import Keys._
import Process._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

object LegoBuild extends Build {

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test    := formattingPreferences
  )

  lazy val defaults = Project.defaultSettings ++ formatSettings ++ Seq(
    resolvers +=  "OSSH" at "https://oss.sonatype.org/content/groups/public",
    resolvers += Resolver.sonatypeRepo("snapshots"),

    // add the library, reflect and the compiler as libraries
    libraryDependencies ++= Seq(
      "junit" % "junit-dep" % "4.10" % "test",
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test",
      "org.scala-lang"         %  "scala-reflect" % "2.11.1",
      "org.scala-lang" % "scala-compiler" % "2.11.1" % "optional"
    ),

    // add scalac options (verbose deprecation warnings)
    scalacOptions ++= Seq("-deprecation", "-feature"),

    resourceDirectory in Compile <<= baseDirectory / "config",

    unmanagedSourceDirectories in Compile += baseDirectory.value / "java-src",

    // testing
    parallelExecution in Test := false,
    fork in Test := false,
    scalaVersion := "2.11.1"
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
  }

  lazy val lego        = Project(id = "root",                     base = file("."), settings = defaults) aggregate (lego_core, legolifter, legocompiler)
  lazy val lego_core       = Project(id = "lego-core",        base = file("lego")  , settings = defaults ++ 
    Seq(name := "lego-core"))
  lazy val legolifter = Project(id = "legolifter", base = file("legolifter"), settings = defaults ++ Seq(name := "legolifter",
      libraryDependencies += "ch.epfl.data" % "autolifter_2.11" % "0.1-SNAPSHOT")) dependsOn(lego_core)
  lazy val legocompiler = Project(id = "legocompiler", base = file("legocompiler"), settings = defaults ++ Seq(name := "legocompiler",
      libraryDependencies ++= Seq("ch.epfl.lamp" % "yin-yang_2.11" % "0.1-SNAPSHOT",
        "ch.epfl.data" % "pardis-core_2.11" % "0.1-SNAPSHOT"
        ))) dependsOn(lego_core)
}
