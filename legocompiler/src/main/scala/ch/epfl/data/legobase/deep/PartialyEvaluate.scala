package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._

/**
 * it does offline partial evaluation
 */
class PartialyEvaluate(override val IR: LoweringLegoBase) extends Optimizer[LoweringLegoBase](IR) {
  import IR._

  sealed trait Phase
  case object BTA extends Phase

  var phase: Phase = BTA

  sealed trait Stage {
    def and(s: Stage): Stage
  }
  case object Static extends Stage {
    def and(s: Stage): Stage = s
  }
  case object Dynamic extends Stage {
    def and(s: Stage): Stage = Dynamic
  }

  def optimize[T: Manifest](node: Block[T]): Block[T] = {
    // TODO first an escape analysis phase should be added
    phase = BTA
    contextIsStatic = false
    traverseBlock(node)
    debug()
    transformProgram(node)
  }

  def debug() {
    Predef.println("Binding time information")
    Predef.println("========================")
    bindingMap.toList.filter(_._1.isInstanceOf[Sym[_]]).map(x => x._1.asInstanceOf[Sym[Any]] -> x._2).sortBy(_._1.id).foreach {
      case (fa, st) =>
        fa match {
          case Sym(id) => Predef.println(id + ":" + st)
          case _       => ()
        }
    }
  }
  var contextIsStatic = false
  val bindingMap = collection.mutable.Map[FunctionArg, Stage]()
  val dependancyChain = collection.mutable.Map[FunctionArg, List[Sym[Any]]]()
  val mutableBindingChain = collection.mutable.Map[Var[Any], List[Sym[Any]]]()
  val mutableBindingMap = collection.mutable.Map[Var[Any], Stage]()
  val mutableContextLevel = collection.mutable.Map[Var[Any], Int]()
  val cfgContext = collection.mutable.Stack[Stage]()

  type Value = Any

  val environment = collection.mutable.Map[FunctionArg, Value]()
  val mutableValues = collection.mutable.Map[Var[Any], Value]()

  override def traverseStm(stm: Stm[_]): Unit = stm match {
    case Stm(sym, rhs) => {
      phase match {
        case BTA => bindingTimeAnalysis(sym, rhs)
      }
    }
  }

  def addContext(stage: Stage) {
    cfgContext.push(stage)
  }

  def popContext() {
    cfgContext.pop()
  }

  def addVarContext(v: Var[Any]) {
    mutableContextLevel(v) = cfgContext.size - 1
  }

  def varContextIsDynamic(v: Var[Any]): Boolean = {
    val varContext = cfgContext.reverse.drop(mutableContextLevel(v) + 1)
    // Predef.println(s"var context for $v=$varContext")
    !varContext.forall(_ == Static)
  }

  def bindingTimeAnalysis[T](sym: Sym[T], rhs: Def[T]): Unit = {
    // val saveContext = contextIsStatic
    // val stage = computeStageDef(rhs)
    val stage = rhs match {
      case Block(stmts, res) => {
        stmts.foreach(x => traverseStm(x))
        if (stmts.forall(_.sym.isStatic) && res.isStatic)
          Static
        else
          Dynamic
      }
      case While(cond, rest) => {
        bindingTimeAnalysis(null, cond) // FIXME
        addContext(cond.stage)
        // if (cond.isDynamic) {
        //   contextIsStatic ^= false
        // }
        bindingTimeAnalysis(null, rest)
        popContext()
        cond.stage and rest.stage
      }
      case IfThenElse(cond, thenp, elsep) => {
        addContext(cond.stage)
        bindingTimeAnalysis(null, thenp)
        bindingTimeAnalysis(null, elsep)
        popContext()
        cond.stage and thenp.stage and elsep.stage
      }
      case NewVar(init) => {
        val v = Var(sym.asInstanceOf[Sym[Var[Any]]])
        addVarContext(v)
        mutableBindingChain(v) = List(sym)
        v.stage = init.stage
        // Predef.println(v + " added with stage " + v.stage)
        v.stage
      }
      case ReadVar(v) => {
        // Predef.println(mutableBindingChain)
        v.addChain(sym)
        v.stage
      }
      case Assign(v, newValue) => {
        v.addChain(sym)
        if (v.isStatic && (newValue.isDynamic || varContextIsDynamic(v) /* || !contextIsStatic*/ )) {
          // Predef.println(s"$v changed to dynamic because of $newValue")
          v.stage = Dynamic
        }
        v.stage
      }
      case _ => {
        for (fa <- rhs.funArgs) {
          fa match {
            case d: Def[_]       => bindingTimeAnalysis(null, d)
            case s: Sym[_]       => if (!bindingMap.contains(s)) s.stage = Dynamic
            case c: Constant[_]  => ()
            case PardisVarArg(f) => ()
          }
        }
        if (rhs.isPure && rhs.funArgs.forall(_.isStatic)) Static else Dynamic
      }
    }
    if (sym != null) {
      for (fa <- rhs.funArgs) {
        fa.addChain(sym)
      }

      sym.stage = stage
    } else {
      rhs.asInstanceOf[FunctionArg].stage = stage
    }
    // dependancyChain(sym) = List()
    // contextIsStatic = saveContext
  }

  implicit class VarOps[T](v: Var[T]) {
    def isStatic: Boolean = stage == Static
    def isDynamic: Boolean = stage == Dynamic
    def stage: Stage = mutableBindingMap(v)
    def stage_=(s: Stage): Unit = {
      mutableBindingMap(v) = s
      if (mutableBindingChain.contains(v)) {
        mutableBindingChain(v) foreach { fa =>
          fa.stage = s
        }
      }
    }
    def addChain[T](sym: Sym[T]): Unit = {
      mutableBindingChain(v) +:= sym
    }

    def value_=(newValue: Value): Unit = {
      mutableValues(v) = newValue
    }

    def value: Value = mutableValues(v)
  }

  implicit class FunctionArgOps(v: FunctionArg) {
    def isStatic: Boolean = stage == Static
    def isDynamic: Boolean = stage == Dynamic
    def stage: Stage = v match {
      case Constant(_) => Static
      case _           => bindingMap(v)
    }
    def stage_=(s: Stage): Unit = {
      val prevStage = bindingMap.get(v)
      if (prevStage.isEmpty || prevStage.get != s) {
        bindingMap(v) = s
        if (dependancyChain.contains(v)) {
          dependancyChain(v) foreach { fa =>
            fa.stage = s
          }
        } else {
          dependancyChain(v) = List()
        }
      }
    }
    def addChain[T](sym: Sym[T]): Unit = {
      v match {
        case Constant(_)     => ()
        case PardisVarArg(f) => f.addChain(sym)
        case _               => dependancyChain(v) +:= sym
      }
    }

    def value_=(newValue: Value): Unit = {
      environment(v) = newValue
    }

    def value: Value = v match {
      case Constant(vv) => vv
      case _            => environment(v)
    }
  }

  override def transformStmToMultiple(stm: Stm[_]): List[to.Stm[_]] = stm match {
    case Stm(s, d) if s.isStatic => d match {
      case NewVar(init) => {
        val initValue = init.value
        val v = Var(s.asInstanceOf[Sym[Var[Any]]])
        v.value = initValue
        Nil
      }
      case ReadVar(v) => {
        // Predef.println(mutableBindingChain)
        // v.addChain(sym)
        // v.stage
        s.value = v.value
        Nil
      }
      case Assign(v, newValue) => {
        // v.addChain(sym)
        // if (v.isStatic && (newValue.isDynamic || varContextIsDynamic(v) /* || !contextIsStatic*/ )) {
        //   // Predef.println(s"$v changed to dynamic because of $newValue")
        //   v.stage = Dynamic
        // }
        // v.stage
        v.value = newValue.value
        Nil
      }
      case _ => super.transformStmToMultiple(stm)
    }
    case _ => super.transformStmToMultiple(stm)
  }
}
