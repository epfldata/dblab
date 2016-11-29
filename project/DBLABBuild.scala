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
      "log4j" % "log4j" % "1.2.17",
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
  val sc_version = "0.1.31-SNAPSHOT"

  // addCommandAlias("test-gen", ";project legobase; project root; clean")

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
        "ch.epfl.data" % "sc-pardis-quasi_2.11" % sc_version,
        "ch.epfl.data" % "squid-sc-backend_2.11" % "0.1-SNAPSHOT"
        ))) dependsOn dblab_components

  lazy val dblab_experimentation       = Project(id = "dblab-experimentation",        base = file("experimentation"),
   settings = defaults ++ purgatorySettings("experimentation/compiler") ++  Seq(
     name := "dblab-experimentation",
     scalacOptions ++= Seq("-optimize"))) dependsOn dblab_components

  lazy val dblab_experimentation_compiler       = Project(id = "dblab-experimentation-compiler",        base = file("experimentation/compiler"),
   settings = defaults ++ Seq(
     name := "dblab-experimentation-compiler")) dependsOn (dblab_experimentation, dblab_components_compiler)

  lazy val dblab            = Project(id = "root",             base = file("."), settings = defaults) aggregate (dblab_components, dblab_experimentation, 
    dblab_components_compiler, dblab_experimentation_compiler, legobase)

  val legobaseMainClass = "ch.epfl.data.dblab.legobase.experimentation.tpch.TPCHCompiler"

  lazy val legobase = Project(id = "legobase", base = file("systems/legobase"), settings = defaults ++ Seq(name := "legobase",
      mainClass in Compile := Some(legobaseMainClass),
      generate_test <<= inputTask { (argTask: TaskKey[Seq[String]]) =>
        (argTask, sourceDirectory in Test, fullClasspath in Compile, runner in Compile, streams) map { (args, srcDir, cp, r, s) =>
          if(args(2).startsWith("Q")) {
            val cgDir = srcDir / "scala" / "generated"
            IO.delete(cgDir ** "*.scala" get)
            toError(r.run(legobaseMainClass, cp.files, args :+ "-scala", s.log))
            val fileName = args(2) + "_Generated.scala"
            val filePath = cgDir / fileName
            println("Generated " + fileName)
            IO.copyFile(new java.io.File("generator-out") / (args(2) + ".scala"), filePath)
            println(s"Run it using `test:runMain ch.epfl.data.dblab.${args(2)} ${args(0)} ${args(1)} ${args(2)}`")
          } else if (args(2) == "testsuite-scala") {
            for(i <- 1 to 22) {
              val newArgs = args.dropRight(1) :+ s"Q$i"
              toError(r.run(legobaseMainClass, cp.files, newArgs :+ "-scala", s.log))  
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
      scalacOptions in Test ++= Seq("-optimize"))) dependsOn(dblab_experimentation_compiler)
}
