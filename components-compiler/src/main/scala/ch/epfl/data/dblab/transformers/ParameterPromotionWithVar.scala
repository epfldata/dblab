package ch.epfl.data
package dblab
package transformers

import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import scala.collection.mutable
import utils.Logger

class ParameterPromotionWithVar[Lang <: Base](override val IR: Lang) extends ParameterPromotion(IR) with StructCollector[Lang] {
  import IR._
  override def analyse[T: TypeRep](node: Block[T]): Unit = {
    super.analyse(node)
    promotedObjects.retain(_.tp match {
      case tp if tp.isVariable && tp.typeArguments(0).isRecord => true
      case tp if tp.isRecord => true
      case _ => false
    })
    // debug
    // symsState.toList.sortBy(_._1.id) foreach {
    //   case (sym, state) =>
    //     Logger[ParameterPromotionWithVar[_]].debug(s"${sym.id}: ${sym.tp} => $state")
    // }
  }

  case class VarStruct(variables: Map[String, Var[_]]) {
    def read[T](field: String): Rep[T] = {
      val variable = variables(field).asInstanceOf[Var[Any]]
      // Logger[ParameterPromotionWithVar[_]].debug(s"$field -> ${variable.e.tp}")
      readVar(variable)(variable.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]).asInstanceOf[Rep[T]]
    }
  }

  val varsStruct = collection.mutable.Map[Var[_], VarStruct]()

  rewrite += statement {
    case sym -> NewVar(e) if promotedObjects.contains(sym) =>
      e match {
        case Def(Struct(t, elems, _)) =>
          val vars = for (elem <- elems) yield {
            val v = __newVarNamed(elem.init, elem.name)(elem.init.tp)
            elem.name -> v
          }
          varsStruct += Var(sym.asInstanceOf[Rep[Var[Any]]]) -> VarStruct(vars.toMap)
        case Constant(x) if x == null || x == 0 => {
          val structTp = sym.tp.typeArguments(0)
          getStructDef(structTp) match {
            case Some(structDef) => {
              val vars = for (elem <- structDef.fields) yield {
                val v = __newVarNamed(unit(DefaultValue(elem.tpe.name)), elem.name)(elem.tpe)
                // Logger[ParameterPromotionWithVar[_]].debug(s"var $sym = $e ===> ${elem.name} -> ${elem.tpe}")
                elem.name -> v
              }
              varsStruct += Var(sym.asInstanceOf[Rep[Var[Any]]]) -> VarStruct(vars.toMap)
            }
            case None =>
              throw new Exception(s"No struct definition found for `var $sym: ${sym.tp.typeArguments(0)} = $e`")
          }
        }
      }
      sym
  }

  rewrite += removeStatement {
    case sym -> ReadVar(v) if promotedObjects.contains(sym) =>
      ()
  }

  rewrite += rule {
    case Assign(sym, rhs) if promotedObjects.contains(sym.e.asInstanceOf[Sym[_]]) =>
      rhs match {
        case Def(Struct(t, elems, _)) =>
          val varStruct = varsStruct(sym)
          for (elem <- elems) {
            __assign(varStruct.variables(elem.name), elem.init)(elem.init.tp)
          }
        case Constant(x) if x == null || x == 0 => {
          val varStruct = varsStruct(sym)
          for ((name, variable) <- varStruct.variables) {
            __assign(variable, unit(DefaultValue(variable.e.tp.typeArguments(0).name)))
          }
        }
        case Def(ReadVar(v)) =>
          val varStruct1 = varsStruct(sym)
          val varStruct2 = varsStruct(v)
          for ((name, variable) <- varStruct1.variables) {
            val e = varStruct2.read[Any](name)
            __assign(variable, e)(e.tp)
          }
        case _ => sys.error(s"`$sym = $rhs` cannot be parameter promoted!")
        //   val structTp = sym.tp.typeArguments(0)
        //   getStructDef(structTp) match {
        //     case Some(structDef) => {
        //       val vars = for (elem <- structDef.fields) yield {
        //         val v = __newVarNamed(unit(DefaultValue(elem.tpe.name)), elem.name)(elem.tpe)
        //         System.out.println(s"===${elem.name} -> ${elem.tpe}")
        //         elem.name -> v
        //       }
        //       varsStruct += Var(sym.asInstanceOf[Rep[Var[Any]]]) -> VarStruct(vars.toMap)
        //     }

        //   }
        // }
      }
      unit()
  }

  override def getParameter[T: TypeRep](obj: Rep[T], name: String): Rep[Any] = obj match {
    case Def(Struct(t, elems, _)) => elems.find(_.name == name) match {
      case Some(elem) => elem.init
      case None       => sys.error(s"In Parameter Promotion, no field found with the name `$name` for type ${typeRep[T]}")
    }
    case Def(ReadVar(v)) =>
      varsStruct.get(v) match {
        case Some(varStruct) => {
          varStruct.read(name)
        }
        case None =>
          sys.error(s"Reading variable $v not supported yet!")
      }

  }

  def isUnbreakableNode[T](rhs: Def[T]): Boolean = rhs match {
    case sc.pardis.deep.scalalib.OptionIRs.OptionGet(_) => true
    case sc.pardis.deep.scalalib.ArrayIRs.ArrayApply(_, _) => true
    case _ => false
  }

  override def escapeAnalysis[T](sym: Sym[T], rhs: Def[T]): Unit = {
    rhs match {
      case _: ConstructorDef[_]  => sym.initialized
      case PardisStruct(_, _, _) => sym.initialized
      case Block(stmts, res) => res match {
        case s: Sym[_] => sym.addChain(s)
        case _         =>
      }
      case NewVar(init) => {
        init match {
          case s: Sym[_] => sym.addChain(s)
          case _         =>
        }
        sym.initialized
      }
      case ReadVar(v) => {
        sym.addChain(v.e.asInstanceOf[Sym[_]])
      }
      case Assign(v, value: Sym[_]) => {
        val vSym = v.e.asInstanceOf[Sym[_]]
        if (value.isInitialized && value.state == Escaped) {
          vSym.state = Escaped
        } else {
          vSym.addChain(value)
        }
      }
      case ReadVal(sy) => {
        sy match {
          case s: Sym[_] => sym.addChain(s)
          case _         =>
        }
      }
      case _ if isUnbreakableNode(rhs) =>
        sym.markAsEscaped
      case _ => ()
    }
    val arguments = rhs match {
      case _: ImmutableFieldAccess[_] | _: FieldGetter[_] => Nil
      case fs: FieldSetter[_] => List(fs.newValue)
      case Assign(_, _) => Nil
      case ReadVar(_) => Nil
      case NewVar(_) => Nil
      // case NotEqual(_, _) => Nil
      // case Equal(_, _) => Nil
      case _ => rhs.funArgs
    }
    arguments foreach {
      case sym: Sym[_] => sym.markAsEscaped
      case _           =>
    }
  }
}
