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
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

// TODO it should be generalized to not be only TPCH-specific

@metadeep(
  folder = "",
  header = """import ch.epfl.data.dblab.legobase.deep._
import ch.epfl.data.dblab.legobase.deep.queryengine._
import ch.epfl.data.dblab.legobase.deep.tpch._""",
  component = "",
  thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
class MetaInfo

@needs[(K2DBScanner, Array[_], REGIONRecord, PARTSUPPRecord, PARTRecord, NATIONRecord, SUPPLIERRecord, LINEITEMRecord, ORDERSRecord, CUSTOMERRecord, OptimalString)]
@deep
trait Loader

/**
 * A module that defines loaders for relations.
 *
 * (TODO for now it's specific to TPCH, but should be generalized)
 */
object Loader {

  val tpchSchema = TPCHSchema.getSchema(Config.datapath, getScalingFactor)
  def getTable(name: String) = tpchSchema.tables.find(t => t.name == name)
  def getScalingFactor = Config.datapath.slice(Config.datapath.lastIndexOfSlice("sf") + 2, Config.datapath.length - 1).toDouble //TODO Pass SF to Config

  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName

  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
  }

  def constructorArgs[T](implicit tt: TypeTag[T]) =
    tt.tpe.member(termNames.CONSTRUCTOR).asMethod.paramLists.head map {
      p => (p.name.decodedName.toString, p.typeSignature)
    }

  @dontInline
  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  def loadRegion() = loadTable[REGIONRecord](getTable("REGION").get)

  def loadPartsupp() = loadTable[PARTSUPPRecord](getTable("PARTSUPP").get)

  def loadPart() = loadTable[PARTRecord](getTable("PART").get)

  def loadNation() = loadTable[NATIONRecord](getTable("NATION").get)

  def loadSupplier() = loadTable[SUPPLIERRecord](getTable("SUPPLIER").get)

  def loadLineitem() = loadTable[LINEITEMRecord](getTable("LINEITEM").get)

  def loadOrders() = loadTable[ORDERSRecord](getTable("ORDERS").get)

  def loadCustomer() = loadTable[CUSTOMERRecord](getTable("CUSTOMER").get)

  // TODO implement the loader method with the following signature.
  // This method works as follows:
  //   1. Converts typeTag[R] into a table
  //   2. Invokes the other method
  // def loadTable[R](implicit t: TypeTag[R]): Array[R]

  // TODO
  // def loadTable[R](schema: Schema)(implicit t: TypeTag[R]): Array[R] = {

  def loadTable[R](table: Table)(implicit t: TypeTag[R]): Array[R] = {
    implicit val c: reflect.ClassTag[R] = reflect.ClassTag[R](t.mirror.runtimeClass(t.tpe))
    val size = fileLineCount(table.resourceLocator)
    val arr = new Array[R](size)
    val ldr = new K2DBScanner(table.resourceLocator)
    val recordType = typeOf[R]

    val classMirror = currentMirror.reflectClass(recordType.typeSymbol.asClass)
    val constr = recordType.decl(termNames.CONSTRUCTOR).asMethod
    val recordArguments = constructorArgs[R]

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