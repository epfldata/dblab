package ch.epfl.data
package dblab.legobase
package deep

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import scala.reflect._
import scala.reflect.runtime.universe.{ termNames, typeOf, TermName }
import scala.reflect.runtime.currentMirror
import dblab.legobase.schema._

// TODO volcano cakes should be added whenever we have support for both volcano and push engines
// trait InliningLegoBase extends volcano.InliningVolcano with DeepDSL with sc.pardis.ir.InlineFunctions with tpch.QueriesImplementations with LoopUnrolling with InliningLoader
/** A polymorphic embedding cake which chains all cakes responsible for further partial evaluation. */
trait InliningLegoBase extends queryengine.push.InliningPush with DeepDSL with sc.pardis.ir.InlineFunctions with tpch.QueriesImplementations with LoopUnrolling with InliningLoader

/** A polymorphic embedding cake for manually inlining some methods of [[ch.epfl.data.dblab.legobase.storagemanager.Loader]] */
trait InliningLoader extends storagemanager.LoaderImplementations with tpch.TPCHLoaderImplementations { this: InliningLegoBase =>
  override def loaderGetFullPathObject(fileName: Rep[String]): Rep[String] = fileName match {
    case Constant(name: String) => unit(Config.datapath + name)
    case _                      => throw new Exception(s"file name should be constant but here it is $fileName")
  }
  override def printOp_Field_PrintQueryOutput[A](self: Rep[PrintOp[A]])(implicit typeA: TypeRep[A]): Rep[Boolean] = unit(Config.printQueryOutput)
  override def tPCHLoaderGetTableObject(tableName: Rep[String]): Rep[Table] = tableName match {
    case Constant(v) => unit(dblab.legobase.tpch.TPCHLoader.getTable(v))
  }
  override def loaderLoadTableObject[R](_table: Rep[Table])(implicit typeR: TypeRep[R], c: ClassTag[R]): Rep[Array[R]] = {
    val table = _table match {
      case Constant(v: Table) => v
    }
    val size = Loader.fileLineCount(unit(table.resourceLocator))
    val arr = __newArray[R](size)
    val ldr = __newK2DBScanner(unit(table.resourceLocator))
    val recordType = currentMirror.staticClass(c.runtimeClass.getName).asType.toTypeConstructor

    val classMirror = currentMirror.reflectClass(recordType.typeSymbol.asClass)
    // val constr = recordType.decl(termNames.CONSTRUCTOR).asMethod
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

    // val constructorMethod = recordType.decls.find(_.name.decodedName.toString == "__new" + c.runtimeClass.getSimpleName)
    // val constructorMethod = typeOf[DeepDSL].decls.find(_.name.decodedName.toString == "__new" + c.runtimeClass.getSimpleName)
    val reflectThis = currentMirror.reflect(this)
    val constructorMethod = typeOf[DeepDSL].members.find(_.name.decodedName.toString == "__new" + c.runtimeClass.getSimpleName).get.asMethod
    val reflectedMethod = reflectThis.reflectMethod(constructorMethod)
    // val constructorMethod = typeOf[DeepDSL].decl(TermName("__new" + c.runtimeClass.getSimpleName))

    // System.out.println(typeOf[DeepDSL].decls.toList)

    val i = __newVar(unit(0))
    __whileDo(((i: Rep[Int]) < size) && ldr.hasNext(), {
      val values = arguments.map(arg =>
        arg._3.dataType match {
          case IntType          => ldr.next_int
          case DoubleType       => ldr.next_double
          case CharType         => ldr.next_char
          case DateType         => ldr.next_date
          case VarCharType(len) => Loader.loadString(len, ldr)
        })

      reflectedMethod.apply(values: _*) match {
        case rec: Rep[R] => arr(i) = rec
        case _           => throw new ClassCastException
      }
      // arr((i: Rep[Int])) = unit(constructorMethod.toString).asInstanceOf[Rep[R]]
      __assign(i, (i: Rep[Int]) + unit(1))
    })
    arr
  }

}

/** A polymorphic embedding cake for performing loop unrolling in the case of using var args. */
trait LoopUnrolling extends sc.pardis.ir.InlineFunctions { this: InliningLegoBase =>
  override def seqForeach[A, U](self: Rep[Seq[A]], f: Rep[A => U])(implicit typeA: TypeRep[A], typeU: TypeRep[U]): Rep[Unit] = self match {
    case Def(LiftedSeq(elems)) => elems.toList match {
      case Nil => unit(())
      case elem :: tail => {
        __app(f).apply(elem)
        __liftSeq(tail).foreach(f)
      }
    }
  }
}

