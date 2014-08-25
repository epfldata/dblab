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

  // we can make an SBT plugin out of these. However, for the time being this is good.
  val generatorMode = SettingKey[Boolean]("generator-mode",
    "Is the compiler used for generating the deep embedding")

  def embed = Command.command("embed") { state =>
    val cleaned = Project.runTask(Keys.clean in Compile, state)
    cleaned match {
      case Some((state, _)) =>
        Project.evaluateTask(Keys.compile in Compile,
          (Project extract state).append(Seq(generatorMode := true), state))
        Project.evaluateTask(Keys.clean in Compile, state)
        state
      case None =>
        state
    }
  }

  def generatorSettings: Seq[Setting[_]] = Seq(
    libraryDependencies += "ch.epfl.data" % "autolifter_2.11" % "0.1-SNAPSHOT",
    generatorMode := false,
    scalacOptions ++= {
      if(generatorMode.value) {
        val cpath = update.value.matching(configurationFilter()).classpath
        val plugin = cpath.files.find(_.getName contains "autolifter").get.absString
        val yy_core = cpath.files.find(_.getName contains "yy-core").get.absString
        val yy = cpath.files.find(_.getName contains "yin-yang").get.absString
        Seq(
          s"-Xplugin:$plugin:$yy_core:$yy",
          "-Ystop-after:backend-generator"
        )
      } else
        Seq()
    },
    commands += embed
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

  lazy val lego            = Project(id = "root",             base = file("."), settings = defaults) aggregate (lego_core, legolifter, legocompiler)
  lazy val lego_core       = Project(id = "lego-core",        base = file("lego"),
   settings = defaults ++ generatorSettings ++  Seq(name := "lego-core",  scalacOptions ++= Seq("-optimize"))) // hack for being able to generate implementations
  lazy val legolifter = Project(id = "legolifter", base = file("legolifter"),
    settings = defaults ++ generatorSettings ++ Seq(name := "legolifter"))
    .dependsOn(lego_core)
  lazy val legocompiler = Project(id = "legocompiler", base = file("legocompiler"), settings = defaults ++ Seq(name := "legocompiler",
      libraryDependencies ++= Seq("ch.epfl.lamp" % "yin-yang_2.11" % "0.1-SNAPSHOT",
        "ch.epfl.data" % "pardis-core_2.11" % "0.1-SNAPSHOT"
        ),
      scalacOptions in Test ++= Seq("-optimize"))) dependsOn(lego_core)
}
