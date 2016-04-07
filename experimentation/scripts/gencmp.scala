#!/usr/bin/env scalas
 
/***

scalaVersion := "2.11.7"

resolvers += Resolver.typesafeIvyRepo("releases")
 
libraryDependencies += "org.scala-sbt" % "io" % "0.13.8"

*/

import sys.process._
import sbt._, Path._
import java.io.File
import java.net.{URI, URL}
def file(s: String): File = new File(s)
def uri(s: String): URI = new URI(s)

val numQueries = 22
val queryNums = (1 to numQueries)

def genc(outputDir: String, compareDir: Option[String] = None, gen: Boolean = true) {
  
  if (gen) {
  	  val out = file(outputDir)
  	  // if (out exists) s"rm -f $outputDir/*".! // doesn't work in zch (asks for confirmation)
  	  if (out exists) (out ** "*.c").get foreach (f => s"rm $f" !)
	  else s"mkdir $outputDir" !
	  
	  val in = file(s"$outputDir/in") /* TODO simplify: make the sbt command
			val pseq = Seq("/home/bob/sbt.sh", "project Foo", "run")
			val pb = scala.sys.process.Process(pseq, new java.io.File("/home/bob/workingdir"))
		or see this: http://stackoverflow.com/questions/13335010/redirect-string-to-scala-sys-process
	  */
	  
	  "echo project lego-compiler" #> in !
	  
	  for (i <- queryNums)
	  	s"echo run DATA_FOLDER 1 Q$i -optimal" #>> in !
	  
	  "echo exit" #>> in !
	  
	  // Launches sbt with large memory
	  "java -Xmx1G -XX:MaxPermSize=2G -jar /Users/lptk/.conscript/sbt-launch.jar" #< in !
	  
	  for (f <- (file("generator-out") ** "*.c").get) {
	  	println(s"moving $f => $outputDir")
	  	s"mv $f $outputDir" !
	  }
  }
  
  compareDir foreach { compareDir =>
  	var totLines = 0
  	
  	val compareDirName = compareDir split "/" last
  	
  	val cmpDir = file(s"$outputDir/cmp_$compareDirName")
  	
  	if (cmpDir exists) (cmpDir ** "*").get foreach (f => s"rm $f" !)
	  else s"mkdir $cmpDir" !
	
  	for (i <- queryNums) {
  		println(s"Comparing Q$i.c ...")
  		
	  	val cmp = file(s"$cmpDir/cmp$i")
	  	
	  	if (cmp exists) s"rm $cmp" !
	  	
  		s"diff $outputDir/Q$i.c $compareDir/Q$i.c" #>> cmp !
  		
	  	val cmpLines = scala.io.Source.fromFile(cmp).getLines.toArray
	  	if (cmpLines.mkString != "") {
	  		val dl = cmpLines.size/4
	  		totLines += dl
	  		System.err.println(s"/!\\ Differences found: $dl lines.")
	  		System.err.println(s"/!\\ (Results in file '$cmp')")
	  		// System exit -1
	  	}
  	}
  	
  	if (totLines ==0) System.out.println("Good! No differences found.")
  	else System.out.println(s"\n\t/!\\ Differences found! $totLines line(s) in total.\n")
  }
  
  
  
}
args match {
  case Array("gen", outputDir) => genc(outputDir)
  case Array("cmp", outputDir, compareDir) => genc(outputDir, Some(compareDir), false)
  case Array("gencmp", outputDir, compareDir) => genc(outputDir, Some(compareDir))
  case _ =>
  	System.err.println("Usage:")
  	System.err.println("	gencmp gen    outputDir")
  	System.err.println("	gencmp cmp    outputDir compareDir")
  	System.err.println("	gencmp gencmp outputDir compareDir")
}
