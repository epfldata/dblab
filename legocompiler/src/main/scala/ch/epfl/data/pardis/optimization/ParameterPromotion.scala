package ch.epfl.data
package pardis
package optimization

import legobase.deep._
import scala.language.implicitConversions
import pardis.ir._

abstract class ParameterPromotion[Lang <: Base](override val IR: Lang) extends Optimizer[Lang](IR) {
  import IR._

  sealed trait Phase
  case object EscapeAnalysis extends Phase
  case object MarkSymbols extends Phase

  var phase: Phase = EscapeAnalysis

  def optimize[T: Manifest](node: Block[T]): Block[T] = {
    // TODO first an escape analysis phase should be added
    phase = EscapeAnalysis
    traverseBlock(node)
    phase = MarkSymbols
    traverseBlock(node)
    transformProgram(node)
  }

  val promotedObjects = collection.mutable.Set[Sym[Any]]()

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      phase match {
        case EscapeAnalysis => escapeAnalysis(sym, rhs)
        case MarkSymbols    => () // FIXME
      }

      super.traverseStm(stm)
    }
  }

  def escapeAnalysis[T](sym: Sym[T], rhs: Def[T]): Unit

  // override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
  //   case Stm(s, node) => node match {
  //     case StructImmutableField(self @ LoweredNew(d), fieldName) => {
  //       subst += s -> transformExp(getParameter(self, fieldName))
  //       Nil
  //     }
  //     case StructFieldGetter(self @ LoweredNew(d), fieldName) => {
  //       getParameter(self, fieldName) match {
  //         case Def(ReadVar(v)) => List(Stm(s, ReadVar(v)(v.tp)))
  //         case _               => ???
  //       }
  //     }
  //     case StructFieldSetter(self @ LoweredNew(d), fieldName, rhs) => {
  //       getParameter(self, fieldName) match {
  //         case Def(ReadVar(v)) => List(Stm(s, Assign(v, rhs)(v.tp)))
  //         case _               => ???
  //       }
  //     }
  //     case _ => {
  //       // Predef.println(s"Cannot do PE for $s with class ${d.getClass}")
  //       super.transformStmToMultiple(stm)
  //     }
  //   }
  // }

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case StructImmutableField(self @ LoweredNew(d), fieldName) => {
      ReadVal(transformExp(getParameter(self, fieldName)))
    }
    case StructFieldGetter(self @ LoweredNew(d), fieldName) => {
      getParameter(self, fieldName) match {
        case Def(ReadVar(v)) => ReadVar(v)(v.tp).asInstanceOf[Def[T]]
        case _               => ???
      }
    }
    case StructFieldSetter(self @ LoweredNew(d), fieldName, rhs) => {
      getParameter(self, fieldName) match {
        case Def(ReadVar(v)) => Assign(v, rhs)(v.tp).asInstanceOf[Def[T]]
        case _               => ???
      }
    }
    case _ => super.transformDef(node)
  }

  def getParameter[T: Manifest](obj: Rep[T], name: String): Rep[Any] = obj match {
    case Def(Struct(t, elems)) => elems.find(_.name == name).map(_.init).get
  }

  object LoweredNew {
    def unapply[T](exp: Rep[T]): Option[Def[T]] = exp match {
      case Def(d) => d match {
        case _ if promotedObjects.contains(exp.asInstanceOf[Sym[Any]]) => Some(d)
        case _ => None
      }
      case _ => None
    }
  }
}
