package miniDB

import Utilities.utilities._
import java.io.PrintWriter
import ch.epfl.data.autolifter._
import ch.epfl.data.autolifter.annotations._
import ch.epfl.data.autolifter.annotations.Custom._

import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.mutable.TreeSet
import QueryEngine.VolcanoPullEngine._
import QueryEngine.GenericEngine._
import sys.process._
import StorageManager.TPCHRelations._
import scala.collection.mutable.DefaultEntry
//trait Engine extends QueryEngine.VolcanoPullEngine

trait ScalaImpl { 
	val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")	
	def parseDate(x: String): Long = { 
        sdf.parse(x).getTime
    }
	def parseString(x: String): Array[Byte] = x.getBytes
}

class miniDB extends ScalaImpl with StorageManager.Loader {
	var currQuery: java.lang.String = ""
	// TODO: This only works for scaling factor 0.1. Make it independent (file existence check needed)
	def getOutputName = currQuery + "Output.txt"
	// TODO: Make this dependant on the scaling factor given
	def getResultFileName = "ref/" + currQuery + ".result"
}

object miniDB extends miniDB with Queries {
	val numRuns: scala.Int = 1
	
	def main(args: Array[String]) {
		LegoBase.Config.datapath = args(0)

		val queries: scala.collection.immutable.List[String] = 
			if (args.length == 2 && args(1) == "testsuite") (for (i <- 1 to 22) yield "Q" + i).toList
			else args.tail.toList
		
        // Test autolift
        val lifter = new AutoLifter(scala.reflect.runtime.universe)
	/*	var liftString = lifter.autoLift[SelectOp[Any]]  + "\n\n" +
                         lifter.autoLift[ScanOp[Any]]    + "\n\n" +
                         lifter.autoLift[AggOp[Any,Any]] + "\n\n" +
                         lifter.autoLift[MapOp[Any]]     + "\n\n" +
                         lifter.autoLift[SortOp[Any]]    + "\n\n" +
                         lifter.autoLift[PrintOp[Any]]   + "\n\n" +
                         lifter.autoLift[AGGRecord[Any]] + "\n\n" +
                         lifter.autoLift[Operator[Any]]  + "\n\n" +
                         lifter.autoLift[Int]            + "\n\n" +
                         lifter.autoLift[Double]         + "\n\n" +
                         lifter.autoLift[Character]      + "\n\n" +
                         lifter.autoLift[Long]           + "\n\n" +
                         lifter.autoLift[Array[Byte]]    + "\n\n" + 
                         lifter.autoLift[LINEITEMRecord] + "\n\n" + 
                         lifter.autoLift[K2DBScanner]    + "\n\n" + 
                         lifter.autoLift[Integer]        + "\n\n" + 
                         lifter.autoLift[Boolean]        + "\n\n" +
                         lifter.autoLift[HashMap[Any,Any]](Custom(List(CMethod("keySet"),CMethod("getOrElseUpdate"), CMethod("size"), CMethod("remove"), CMethod("clear"), CMethod("<init>")))) +
                         lifter.autoLift[Set[Any]](Custom(List(CMethod("apply"), CMethod("toSeq"),CMethod("head"),CMethod("remove")))) +
                         lifter.autoLift[TreeSet[Any]](Custom(List(CMethod("+="),CMethod("-="),CMethod("head"),CMethod("size")))) +
                         lifter.autoLift[DefaultEntry[Any,Any]]*/

        var liftString = lifter.autoLift[HashMap[Any,Any]](Custom(List(CMethod("parCombiner")))) /*+
                         lifter.autoLift[DefaultEntry[Any,Any]]*/
//List(CMethod("collectFirst"))
        liftString = "class Node\n\n" + 
                     "trait Base {\n" +
                     "  type Rep[T] = Node\n" + 
                     "  type Sym[T] = Rep[T]\n" +
                     "  type Contents[T1,T2] = Rep[T1]\n" + 
                     "  type Def[T] = Rep[T]\n" +
                     "  type Block[T] = Rep[T]\n" +
                     "  type Overloaded1 = Array[Any]\n" +
                     "  type Overloaded2 = Int\n" +
                     "  type Overloaded3 = Double\n" +
                     "  type Overloaded4 = String\n" +
                     "  type Overloaded5 = Long\n" +
                     "  type Overloaded6 = Object\n" +
                     "  type Overloaded7 = Char\n" +
                     "  type Overloaded8 = Byte\n" +
                     "  def fresh[T] = { (new Node).asInstanceOf[Rep[T]] }\n" +
                     "}\n\n" + liftString + "\n\n" + 
//                     "trait DeepDSL extends SelectOpComponent with ScanOpComponent with AggOpComponent with MapOpComponent with SortOpComponent with AGGRecordComponent with OperatorComponent with CharacterComponent with DoubleComponent with IntComponent with LongComponent with ArrayComponent with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent with DefaultEntryComponent"
                    "trait DeepDSL extends HashMapComponent" // with DefaultEntryComponent"

        println(liftString)
//        printToFile(new java.io.File("src/main/scala/generated/SelectOp.scala"))(p => p.println(liftString))
        // End of test autolift
		
        for (q <- queries) {
			currQuery = q
		    currQuery match {
			    case "Q1"  => Q1(numRuns)
                case _ => throw new Exception("Query not supported!")
            }
			// Check results
/*			if (checkResults) {
				if (new java.io.File(getResultFileName).exists) {
					val resq = scala.io.Source.fromFile(getOutputName).mkString
					val resc = {
                        val str = scala.io.Source.fromFile(getResultFileName).mkString
                        str * numRuns 
                    }
					if (resq != resc) {
						System.out.println("-----------------------------------------")
						System.out.println("QUERY" + q + " DID NOT RETURN CORRECT RESULT!!!")
						System.out.println("Correct result:")
						System.out.println(resc)
						System.out.println("Result obtained from execution:")
						System.out.println(resq)
						System.out.println("-----------------------------------------")
						System.exit(0)
					} else System.out.println("CHECK RESULT FOR QUERY " + q + ": [OK]")
				} else System.out.println("Reference result file not found. Skipping checking of result")
			}*/
		}
	}
}
