package ch.epfl.data
package dblab.legobase
package storagemanager

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
import schema._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

@metadeep(
  folder = "",
  header = """import ch.epfl.data.dblab.legobase.deep._
import ch.epfl.data.dblab.legobase.deep.queryengine._
import ch.epfl.data.dblab.legobase.schema._
import scala.reflect._
""",
  component = "",
  thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
class MetaInfo

@needs[(K2DBScanner, Array[_], OptimalString)]
@deep
trait Loader

/**
 * A module that defines loaders for relations.
 */
object Loader {

  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName

  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
  }

  @dontInline
  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  // TODO implement the loader method with the following signature.
  // This method works as follows:
  //   1. Converts typeTag[R] into a table
  //   2. Invokes the other method
  // def loadTable[R](implicit t: TypeTag[R]): Array[R]

  // TODO
  // def loadTable[R](schema: Schema)(implicit t: TypeTag[R]): Array[R] = {

  @dontInline
  def loadTable[R](table: Table)(implicit c: ClassTag[R]): Array[R] = {
    val size = fileLineCount(table.resourceLocator)
    val arr = new Array[R](size)
    val ldr = new K2DBScanner(table.resourceLocator)
    val recordType = currentMirror.staticClass(c.runtimeClass.getName).asType.toTypeConstructor

    val classMirror = currentMirror.reflectClass(recordType.typeSymbol.asClass)
    val constr = recordType.decl(termNames.CONSTRUCTOR).asMethod
    val recordArguments = recordType.member(termNames.CONSTRUCTOR).asMethod.paramLists.head map {
      p => (p.name.decodedName.toString, p.typeSignature)
    }

    val arguments = recordArguments.map {
      case (name, tpe) =>
        (name, tpe, table.attributes.find(a => a.name == name) match {
          case Some(a) => a
          case None    => throw new Exception(s"No attribute found with the name `$name` in the table ${table.name}")
        })
    }

    var i = 0
    while (i < size && ldr.hasNext()) {
      val values = arguments.map(arg =>
        arg._3.dataType match {
          case IntType          => ldr.next_int
          case DoubleType       => ldr.next_double
          case CharType         => ldr.next_char
          case DateType         => ldr.next_date
          case VarCharType(len) => loadString(len, ldr)
        })

      classMirror.reflectConstructor(constr).apply(values: _*) match {
        case rec: R => arr(i) = rec
        case _      => throw new ClassCastException
      }
      i += 1
    }
    arr

    //TODO update statistics
  }
}