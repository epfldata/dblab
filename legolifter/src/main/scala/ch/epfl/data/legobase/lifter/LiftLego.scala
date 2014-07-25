package ch.epfl.data
package legobase
package lifter

import scala.reflect.runtime.universe
import universe.typeOf

import queryengine.volcano._
import ch.epfl.data.autolifter._
import ch.epfl.data.autolifter.annotations._
import ch.epfl.data.autolifter.annotations.Custom._
import java.util.{ Calendar, GregorianCalendar }
import scala.collection.mutable.MirrorHashMap
import scala.collection.mutable.MirrorSet
import scala.collection.mutable.MirrorTreeSet
import scala.collection.mutable.MirrorDefaultEntry

object LiftLego {
  val reportToFile = true

  def main(args: Array[String]) {
    implicit val al = new AutoLifter(universe)
    generateLegoBase
    generateNumber
    generateTpe[MirrorArray[_]]
    generateCollection
  }

  val folder = "legocompiler/src/main/scala/ch/epfl/data/legobase/deep"

  def generateNumber(implicit al: AutoLifter) {
    val liftedCodes = List(
      al.autoLift[MirrorInt],
      al.autoLift[MirrorDouble],
      al.autoLift[MirrorCharacter],
      al.autoLift[MirrorLong],
      al.autoLift[MirrorInteger],
      al.autoLift[MirrorBoolean])
    val liftedCode = liftedCodes.mkString("\n")
    val file = "DeepScalaNumber"
    printToFile(new java.io.File(s"$folder/scalalib/$file.scala")) { pw =>
      pw.println(s"""/* Generated by AutoLifter © ${new GregorianCalendar().get(Calendar.YEAR)} */

package ch.epfl.data
package legobase
package deep
package scalalib

import pardis.ir._

$liftedCode
""")
    }
  }

  def generateLegoBase(implicit al: AutoLifter) {
    val liftedCodes = List(
      al.autoLift[queryengine.AGGRecord[Any]],
      al.autoLift[storagemanager.TPCHRelations.LINEITEMRecord],
      al.autoLift[storagemanager.K2DBScanner](Custom(component = "DeepDSL", excludedFields = List(CMethod("br"), CMethod("sdf")))))
    val liftedCode = liftedCodes.mkString("\n")
    val file = "DeepLegoBase"
    printToFile(new java.io.File(s"$folder/$file.scala")) { pw =>
      pw.println(s"""/* Generated by AutoLifter © ${new GregorianCalendar().get(Calendar.YEAR)} */

package ch.epfl.data
package legobase
package deep

import scalalib._
import pardis.ir._

$liftedCode
trait DeepDSL extends OperatorsComponent with AGGRecordComponent with CharacterComponent 
  with DoubleComponent with IntComponent with LongComponent with ArrayComponent 
  with LINEITEMRecordComponent with K2DBScannerComponent with IntegerComponent 
  with BooleanComponent with HashMapComponent with SetComponent with TreeSetComponent 
  with DefaultEntryComponent with ManualLiftedLegoBase
""")
    }
  }

  def generateCollection(implicit al: AutoLifter) {
    val liftedCodes = List(
      al.autoLift[MirrorHashMap[Any, Any]](Custom("DeepDSL")),
      al.autoLift[MirrorSet[Any]](Custom("DeepDSL")),
      al.autoLift[MirrorTreeSet[Any]](Custom("DeepDSL")),
      al.autoLift[MirrorDefaultEntry[Any, Any]](Custom("DeepDSL")))
    val liftedCode = liftedCodes.mkString("\n")
    val file = "DeepScalaCollection"
    printToFile(new java.io.File(s"$folder/scalalib/$file.scala")) { pw =>
      pw.println(s"""/* Generated by AutoLifter © ${new GregorianCalendar().get(Calendar.YEAR)} */

package ch.epfl.data
package legobase
package deep
package scalalib

import pardis.ir._

$liftedCode
""")
    }
  }

  def generateTpe[T: universe.TypeTag](implicit al: AutoLifter) {
    val liftedCode = al.autoLift[T]
    val typeName = implicitly[universe.TypeTag[T]].tpe.typeSymbol.name.toString
    val MIRROR_PREFIX = "Mirror"
    val filePostFix = if (typeName.startsWith(MIRROR_PREFIX)) typeName.drop(MIRROR_PREFIX.size) else typeName
    val file = "DeepScala" + filePostFix
    printToFile(new java.io.File(s"$folder/scalalib/$file.scala")) { pw =>
      pw.println(s"""/* Generated by AutoLifter © ${new GregorianCalendar().get(Calendar.YEAR)} */

package ch.epfl.data
package legobase
package deep
package scalalib

import pardis.ir._

$liftedCode
""")
    }
  }

  /* from http://stackoverflow.com/questions/4604237/how-to-write-to-a-file-in-scala */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    if (reportToFile) {
      val p = new java.io.PrintWriter(f)
      try { op(p) } finally { p.close() }
    } else {
      val p = new java.io.PrintWriter(System.out)
      try { op(p) } finally { p.flush() }
    }
  }
}
