package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._

class MallocProfiler(override val IR: QueryEngineExp) extends RuleBasedTransformer[QueryEngineExp](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  val mallocSyms = scala.collection.mutable.Map[Sym[_], Malloc[_]]()
  val mallocSymVars = scala.collection.mutable.Map[Sym[_], Var[Long]]()
  val mallocSymNewSyms = scala.collection.mutable.Map[Sym[_], Sym[_]]()

  analysis += statement {
    case sym -> (node @ Malloc(num)) =>
      mallocSyms += sym -> node
      ()
  }

  override protected def transformProgram[T: TypeRep](block: Block[T]): Block[T] = block match {
    case Block(stmts, res) => {
      // println(s"processing block with ${stmts.size} and result $res")
      to.reifyBlock[T] {
        for (ms <- mallocSyms) {
          mallocSymVars(ms._1) = __newVarNamed(unit(0L), s"mallocSym_${ms._1.id}_")
        }
        stmts.foreach(transformStmToMultiple)
        val sum = __newVarNamed(unit(0L), "mallocSum")
        for (ms <- mallocSyms) {
          val size = readVar[Long](mallocSymVars(ms._1)) * infix_asInstanceOf[Long](sizeof()(ms._2.tp.typeArguments(0)))
          printf(unit(s"${mallocSymNewSyms(ms._1).id} with type ${ms._1.tp.typeArguments(0).name}: %ld\n"), size)
          __assign(sum, readVar(sum) + size)
        }
        printf(unit("total: %ld\n"), readVar[Long](sum))
        transformExp[T, T](res)
      }
    }
  }

  rewrite += statement {
    case sym -> (node @ Malloc(num)) =>
      val v = mallocSymVars(sym)
      val newSym = malloc(num)(node.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
      mallocSymNewSyms(sym) = newSym.asInstanceOf[Sym[_]]
      __assign(v, readVar(v) + infix_asInstanceOf[Long](num))
      newSym
  }
}
