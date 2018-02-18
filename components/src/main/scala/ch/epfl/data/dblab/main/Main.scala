package ch.epfl.data
package dblab
package main

/**
 * The starting point of dblab-components project.
 *
 * @author Amir Shaikhha
 */
object Main {
  def main(args: Array[String]): Unit = {
    def usage(): Unit = {
      println("Usage: dbtoaster|olap-interpreter [args]")
    }
    if (args.length > 0) {
      args(0) match {
        case "dbtoaster"        => dbtoaster.Driver.main(args.tail)
        case "olap-interpreter" => queryengine.QueryInterpreter.main(args.tail)
        case name =>
          println(s"System $name is not supported")
          usage()
      }
    } else {
      usage()
    }
  }
}
