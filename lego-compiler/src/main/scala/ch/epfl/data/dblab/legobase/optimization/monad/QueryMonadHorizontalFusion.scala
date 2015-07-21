package ch.epfl.data
package dblab.legobase
package optimization
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import compiler._

class QueryMonadHorizontalFusion(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) {
  import IR._

  val candidates = scala.collection.mutable.Set[Rep[Any]]()
  val candidatesNode = scala.collection.mutable.Map[Rep[Any], Def[Any]]()
  val candidatesScope = scala.collection.mutable.Map[Rep[Any], Block[Any]]()

  var currentScope: Block[Any] = _

  override def traverseBlock(block: Block[_]): Unit = {
    val oldScope = currentScope
    currentScope = block.asInstanceOf[Block[Any]]
    super.traverseBlock(block)
    currentScope = oldScope
  }

  analysis += statement {
    case sym -> (node @ QueryFoldLeft(_, _, _)) =>
      // System.out.println(s"$sym $node added to candidates")
      // System.out.println(s"currentScope: ${currentScope.toString.take(24)}")
      candidates += sym
      candidatesNode(sym) = node
      candidatesScope(sym) = currentScope
      ()
  }

  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    val blockIds = candidatesScope.values.toSet.toArray
    val candidatesScopeId = candidatesScope.mapValues(b => blockIds.indexOf(b))
    val result = candidates.groupBy(x => candidatesNode(x).funArgs(0) -> candidatesScopeId(x))
    for ((((range: Rep[Any]), blockId), list) <- result) {
      for (cand <- list) {
        rewrittenOpsRange(cand) = range
        val prevList = rewrittenRangeDefs.getOrElseUpdate(range, Nil)
        rewrittenRangeDefs(range) = prevList :+ (cand -> candidatesNode(cand))
      }
    }
    // System.out.println(s"result $result ${result.size}")
    System.out.println(s"rewrittenOpsRange $rewrittenOpsRange")
  }

  val rewrittenOpsRange = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()
  val rewrittenRangeDefs = scala.collection.mutable.Map[Rep[Any], List[(Rep[Any], Def[Any])]]()
  val alreadyRewrittenRange = scala.collection.mutable.Set[Rep[Any]]()
  val alreadyRewrittenOpsVars = scala.collection.mutable.Map[Rep[Any], Var[Any]]()
}
