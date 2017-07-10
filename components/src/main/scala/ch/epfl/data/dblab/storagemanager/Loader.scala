package ch.epfl.data
package dblab
package storagemanager

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs, ::, onlineInliner, noDeepExt }
import queryengine._
import schema._
import config._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

@metadeep(
  folder = "",
  header = """import ch.epfl.data.dblab.deep._
import ch.epfl.data.dblab.deep.queryengine._
import ch.epfl.data.dblab.deep.schema._
import ch.epfl.data.dblab.schema._
import scala.reflect._
""",
  component = "",
  thisComponent = "")
@needs[FastScanner :: Array[_] :: OptimalString :: String :: Numeric[_] :: DynamicDataRow]
@deep
@noDeepExt
@onlineInliner
trait Loader

/**
 * A module that defines data loading facilities.
 */
object Loader {

  @dontLift
  val cachedTables = collection.mutable.HashMap[Table, Array[_]]()

  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName

  @dontInline
  def loadString(size: Int, s: FastScanner): OptimalString = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
    // s.next_string
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
  def loadUntypedTable(table: Table): Array[DynamicDataRow] = {
    if (Config.cacheLoading && cachedTables.contains(table)) {
      System.out.println(s"Loading cached ${table.name}!")
      cachedTables(table).asInstanceOf[Array[DynamicDataRow]]
    } else {
      val size = fileLineCount(table.resourceLocator)
      val arr = new Array[DynamicDataRow](size)
      val ldr = new FastScanner(table.resourceLocator)

      var i = 0

      val argNames = table.attributes.map(_.name).toSeq

      while (i < size && ldr.hasNext()) {
        val values = table.attributes.map(arg =>
          arg.dataType match {
            case IntType          => ldr.next_int
            case DoubleType       => ldr.next_double
            case DecimalType(_)   => ldr.next_double
            case CharType         => ldr.next_char
            case DateType         => ldr.next_date
            case VarCharType(len) => loadString(len, ldr)
          })
        val rec = new DynamicDataRow(table.name, table.attributes.map(_.name) zip values)
        arr(i) = rec
        i += 1
      }
      if (Config.cacheLoading) {
        cachedTables(table) = arr
      }
      arr
    }

    //TODO update statistics
  }

  @dontLift
  def loadTablePage(table: Table): Array[PageRow] = {
    if (Config.cacheLoading && cachedTables.contains(table)) {
      System.out.println(s"Loading cached ${table.name}!")
      cachedTables(table).asInstanceOf[Array[PageRow]]
    } else {
      val size = fileLineCount(table.resourceLocator)
      val arr = new Array[PageRow](size)
      val ldr = new FastScanner(table.resourceLocator)

      var i = 0

      val argNames = table.attributes.map(_.name).toSeq

      val page = Page(table)

      while (i < size && ldr.hasNext()) {
        val values = table.attributes.map(arg =>
          arg.dataType match {
            case IntType          => ldr.next_int
            case DoubleType       => ldr.next_double
            case DecimalType(_)   => ldr.next_double
            case CharType         => ldr.next_char
            case DateType         => ldr.next_date
            case VarCharType(len) => loadString(len, ldr)
          })

        val rec = PageRow(page, i, values.toArray)
        arr(i) = rec
        i += 1
      }
      if (Config.cacheLoading) {
        cachedTables(table) = arr
      }
      arr
    }

    //TODO update statistics
  }

  @dontInline
  def loadTable[R](table: Table)(implicit c: ClassTag[R]): Array[R] = {
    if (Config.cacheLoading && cachedTables.contains(table)) {
      System.out.println(s"Loading cached ${table.name}!")
      cachedTables(table).asInstanceOf[Array[R]]
    } else {
      val size = fileLineCount(table.resourceLocator)
      val arr = new Array[R](size)
      val ldr = new FastScanner(table.resourceLocator)
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
            case IntType        => ldr.next_int
            case DoubleType     => ldr.next_double
            case DecimalType(_) => ldr.next_double
            case CharType       => ldr.next_char
            case DateType       => ldr.next_date
            case VarCharType(len) => //loadString(len, ldr)
              ldr.next_string
          })

        classMirror.reflectConstructor(constr).apply(values: _*) match {
          case rec: R => arr(i) = rec
          case _      => throw new ClassCastException
        }
        i += 1
      }
      if (Config.cacheLoading) {
        cachedTables(table) = arr
      }
      arr
    }
    //TODO update statistics
  }
}
