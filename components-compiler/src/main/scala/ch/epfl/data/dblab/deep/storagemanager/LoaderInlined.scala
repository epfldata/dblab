package ch.epfl.data
package dblab
package deep
package storagemanager

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import scala.reflect._
import scala.reflect.runtime.universe.{ termNames, typeOf, TermName, Type }
import scala.reflect.runtime.currentMirror
import dblab.schema._
import config._

/** A polymorphic embedding cake for manually inlining some methods of [[ch.epfl.data.dblab.storagemanager.Loader]] */
trait LoaderInlined extends storagemanager.LoaderImplementations
  with schema.SchemaOps with sc.pardis.deep.scalalib.BooleanComponent {
  def componentType: Type = throw new Exception("Provide a componentType for the LoaderInlined component.")
  override def loaderGetFullPathObject(fileName: Rep[String]): Rep[String] = fileName match {
    case Constant(name: String) => unit(Config.datapath + name)
    case _                      => throw new Exception(s"file name should be constant but here it is $fileName")
  }
  override def loaderLoadTableObject[R](_table: Rep[Table])(implicit typeR: TypeRep[R], c: ClassTag[R]): Rep[Array[R]] = {
    val table = _table match {
      case Constant(v: Table) => v
    }
    val size = Loader.fileLineCount(unit(table.resourceLocator))
    val arr = __newArray[R](size)
    val ldr = __newFastScanner(unit(table.resourceLocator))
    val recordType = currentMirror.staticClass(c.runtimeClass.getName).asType.toTypeConstructor

    allTables += table

    val classMirror = currentMirror.reflectClass(recordType.typeSymbol.asClass)
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

    val reflectThis = currentMirror.reflect(this)
    val constructorMethod = componentType.members.find(_.name.decodedName.toString == "__new" + c.runtimeClass.getSimpleName).get.asMethod
    val reflectedMethod = reflectThis.reflectMethod(constructorMethod)

    val i = __newVar(unit(0))
    __whileDo(((i: Rep[Int]) < size) && ldr.hasNext(), {
      val values = arguments.map(arg =>
        arg._3.dataType match {
          case IntType    => ldr.next_int
          case DoubleType => ldr.next_double
          case CharType   => ldr.next_char
          case DateType   => ldr.next_date
          case VarCharType(len) => //Loader.loadString(len, ldr)
            ldr.next_string
        })

      val rec = reflectedMethod.apply(values: _*).asInstanceOf[Rep[R]]
      arr(i) = rec
      __assign(i, (i: Rep[Int]) + unit(1))
    })
    arr
  }

  override def loaderLoadUntypedTableObject(_table: Rep[Table]): Rep[Array[DynamicDataRow]] = {
    val table = _table match {
      case Constant(v: Table) => v
    }
    val size = Loader.fileLineCount(unit(table.resourceLocator))
    val arr = __newArray[DynamicDataRow](size)
    val ldr = __newFastScanner(unit(table.resourceLocator))

    allTables += table

    val argNames = table.attributes.map(_.name).toSeq

    val i = __newVar(unit(0))
    __whileDo(((i: Rep[Int]) < size) && ldr.hasNext(), {
      val values = table.attributes.map(arg =>
        Tuple2(unit(arg.name), arg.dataType match {
          case IntType    => ldr.next_int
          case DoubleType => ldr.next_double
          case CharType   => ldr.next_char
          case DateType   => ldr.next_date
          case VarCharType(len) => //Loader.loadString(len, ldr)
            ldr.next_string
        }))

      val rec = DynamicDataRow(unit(table.name))(values: _*)
      //val rec = reflectedMethod.apply(values: _*).asInstanceOf[Rep[R]]
      arr(i) = rec
      __assign(i, (i: Rep[Int]) + unit(1))
    })
    arr
  }

}
