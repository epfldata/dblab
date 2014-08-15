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

  // addCommandAlias("test-gen", ";project legocompiler; project root; clean")

  val generate_test = InputKey[Unit]("generate-test")
  val test_run = InputKey[Unit]("test-run")

  lazy val lego            = Project(id = "root",             base = file("."), settings = defaults) aggregate (lego_core, legolifter, legocompiler)
  lazy val lego_core       = Project(id = "lego-core",        base = file("lego")  , settings = defaults ++ 
    Seq(name := "lego-core",
      libraryDependencies += "ch.epfl.data" % "autolifter_2.11" % "0.1-SNAPSHOT",
      libraryDependencies += "ch.epfl.data" % "pardis-library_2.11" % "0.1-SNAPSHOT",
      scalacOptions ++= Seq("-optimize"))) // hack for being able to generate implementation
  lazy val legolifter = Project(id = "legolifter", base = file("legolifter"), settings = defaults ++ Seq(name := "legolifter",
      libraryDependencies += "ch.epfl.data" % "autolifter_2.11" % "0.1-SNAPSHOT")) dependsOn(lego_core)
  lazy val legocompiler = Project(id = "legocompiler", base = file("legocompiler"), settings = defaults ++ Seq(name := "legocompiler",
      libraryDependencies ++= Seq("ch.epfl.lamp" % "yin-yang_2.11" % "0.1-SNAPSHOT",
        "ch.epfl.data" % "pardis-core_2.11" % "0.1-SNAPSHOT"
        ),
      generate_test <<= inputTask { (argTask: TaskKey[Seq[String]]) =>
        (argTask, sourceManaged in Test, sourceDirectory in Test, fullClasspath in Test, runner in Test, streams) map { (args, dir, srcDir, cp, r, s) =>
          val cgDir = srcDir / "scala" / "generated"
          IO.delete(cgDir ** "*.scala" get)
          toError(r.run("ch.epfl.data.legobase.compiler.Main", cp.files, args, s.log))
          val fileName = args(2) + "_Generated.scala"
          val filePath = cgDir / fileName
          println("Generated " + fileName)
          IO.copyFile(new java.io.File("generator-out") / "lala.scala", filePath)
          println("Run it using `test-run`")
          // println("classpath:" + (cp.files :+ filePath).mkString("\n"))
          // toError(r.run("ch.epfl.data.legobase.LEGO_QUERY", cp.files/* :+ cgDir*/, args, s.log))
          // test_run.value
        }
      },
      fullRunInputTask(
        test_run
        ,
        Test,
        "ch.epfl.data.legobase.LEGO_QUERY"
      ),
      scalacOptions in Test ++= Seq("-optimize"))) dependsOn(lego_core)
}
