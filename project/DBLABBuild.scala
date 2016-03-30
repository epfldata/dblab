import java.io.File
import sbt._
import Keys._
import Process._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import ch.epfl.data.sc.purgatory.plugin.PurgatoryPlugin._

object DBLABBuild extends Build {

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test    := formattingPreferences
  )

  lazy val defaults = Project.defaultSettings ++ formatSettings ++ Seq(
    resolvers +=  "OSSH" at "https://oss.sonatype.org/content/groups/public",
    // resolvers += Resolver.sonatypeRepo("snapshots"),

    // add the library, reflect and the compiler as libraries
    libraryDependencies ++= Seq(
      "junit" % "junit-dep" % "4.10" % "test",
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test",
      "org.scala-lang"         %  "scala-reflect" % scala_version,
      "org.scala-lang" % "scala-compiler" % scala_version % "optional"
    ),

    // add scalac options (verbose deprecation warnings)
    scalacOptions ++= Seq("-deprecation", "-feature"),

    resourceDirectory in Compile <<= baseDirectory / "config",

    unmanagedSourceDirectories in Compile += baseDirectory.value / "java-src",

    // testing
    parallelExecution in Test := false,
    fork in Test := false,
    scalaVersion := scala_version
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
  }
  val scala_version = "2.11.7"
  val sc_version = "0.1.1-SNAPSHOT"

  // addCommandAlias("test-gen", ";project legocompiler; project root; clean")

  val generate_test = InputKey[Unit]("generate-test")
  // val test_run = InputKey[Unit]("test-run")

  def purgatorySettings(projectFolder: String) = generatorSettings ++ Seq(
    outputFolder := s"$projectFolder/src/main/scala/ch/epfl/data/dblab/deep",
    inputPackage := "ch.epfl.data.dblab",
    outputPackage := "ch.epfl.data.dblab.deep",
    generatePlugins += "ch.epfl.data.sc.purgatory.generator.QuasiGenerator",
    pluginLibraries += "ch.epfl.data" % "sc-purgatory-quasi_2.11" % sc_version) 

  lazy val dblab_components       = Project(id = "dblab-components",        base = file("components"),
   settings = defaults ++ purgatorySettings("components-compiler") ++  Seq(
     name := "dblab-components",
     scalacOptions ++= Seq("-optimize"),
     libraryDependencies += "ch.epfl.data" % "sc-pardis-library_2.11" % sc_version))

  lazy val dblab_components_compiler       = Project(id = "dblab-components-compiler",        base = file("components-compiler"),
   settings = defaults ++ Seq(
     name := "dblab-components-compiler",
     scalacOptions ++= Seq("-optimize"),
     libraryDependencies ++= Seq(//"ch.epfl.lamp" % "scala-yinyang_2.11" % "0.2.0-SNAPSHOT",
        "ch.epfl.data" % "sc-pardis-compiler_2.11" % sc_version,
        "ch.epfl.data" % "sc-c-scala-lib_2.11" % sc_version,
        "ch.epfl.data" % "sc-c-scala-deep_2.11" % sc_version,
        "ch.epfl.data" % "sc-pardis-quasi_2.11" % sc_version
        ))) dependsOn dblab_components

  lazy val dblab_benchmarks       = Project(id = "dblab-benchmarks",        base = file("benchmarks"),
   settings = defaults ++ purgatorySettings("benchmarks-compiler") ++  Seq(
     name := "dblab-benchmarks",
     scalacOptions ++= Seq("-optimize"))) dependsOn dblab_components

  lazy val dblab            = Project(id = "root",             base = file("."), settings = defaults) aggregate (dblab_components, dblab_benchmarks, dblab_components_compiler)

  lazy val legocompiler = Project(id = "lego-compiler", base = file("lego-compiler"), settings = defaults ++ Seq(name := "lego-compiler",
      libraryDependencies ++= Seq(//"ch.epfl.lamp" % "scala-yinyang_2.11" % "0.2.0-SNAPSHOT",
        "ch.epfl.data" % "sc-pardis-compiler_2.11" % sc_version,
        "ch.epfl.data" % "sc-c-scala-lib_2.11" % sc_version,
        "ch.epfl.data" % "sc-c-scala-deep_2.11" % sc_version,
        "ch.epfl.data" % "sc-pardis-quasi_2.11" % sc_version
        ),
      mainClass in Compile := Some("ch.epfl.data.dblab.legobase.tpch.TPCHCompiler"),
      generate_test <<= inputTask { (argTask: TaskKey[Seq[String]]) =>
        (argTask, sourceDirectory in Test, fullClasspath in Compile, runner in Compile, streams) map { (args, srcDir, cp, r, s) =>
          if(args(2).startsWith("Q")) {
            val cgDir = srcDir / "scala" / "generated"
            IO.delete(cgDir ** "*.scala" get)
            toError(r.run("ch.epfl.data.dblab.legobase.tpch.TPCHCompiler", cp.files, args :+ "-scala", s.log))
            val fileName = args(2) + "_Generated.scala"
            val filePath = cgDir / fileName
            println("Generated " + fileName)
            IO.copyFile(new java.io.File("generator-out") / (args(2) + ".scala"), filePath)
            println(s"Run it using `test:run ${args(0)} ${args(1)} ${args(2)}`")
          } else if (args(2) == "testsuite-scala") {
            for(i <- 1 to 22) {
              val newArgs = args.dropRight(1) :+ s"Q$i"
              toError(r.run("ch.epfl.data.dblab.legobase.compiler.Main", cp.files, newArgs :+ "-scala", s.log))  
            }
          }
          // println("classpath:" + (cp.files :+ filePath).mkString("\n"))
          // toError(r.run("ch.epfl.data.legobase.LEGO_QUERY", cp.files/* :+ cgDir*/, args, s.log))
          // test_run.value
        }
      },
      // fullRunInputTask(
      //   test_run
      //   ,
      //   Test,
      //   "ch.epfl.data.legobase.LEGO_QUERY"
      // ),
      scalacOptions in Test ++= Seq("-optimize"))) dependsOn(dblab_components)
}
