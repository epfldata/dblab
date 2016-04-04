package ch.epfl.data
package dblab
package transformers
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

class QueryMonadHorizontalFusion(override val IR: QueryEngineExp) extends RuleBasedTransformer[QueryEngineExp](IR) {
  import IR._

  val candidatesSymbol = scala.collection.mutable.Set[Rep[Any]]()
  val candidatesNode = scala.collection.mutable.Map[Rep[Any], Def[Any]]()
  val candidatesScope = scala.collection.mutable.Map[Rep[Any], Block[Any]]()

  var currentScope: Block[Any] = _

  /**
   * Updates the current scope.
   */
  override def traverseBlock(block: Block[_]): Unit = {
    val oldScope = currentScope
    currentScope = block.asInstanceOf[Block[Any]]
    super.traverseBlock(block)
    currentScope = oldScope
  }

  analysis += statement {
    case sym -> (node @ QueryFoldLeft(_, _, _)) =>
      candidatesSymbol += sym
      candidatesNode(sym) = node
      candidatesScope(sym) = currentScope
      ()
  }

  override def postAnalyseProgram[T: TypeRep](node: Block[T]): Unit = {
    val blockIds = candidatesScope.values.toSet.toArray
    val candidatesScopeId = candidatesScope.mapValues(b => blockIds.indexOf(b))
    // Groups the loop symbols based on their range as well as their block scope.
    val result = candidatesSymbol.groupBy(x => candidatesNode(x).funArgs(0) -> candidatesScopeId(x))
    for ((((range: Rep[Any]), blockId), list) <- result) {
      for (cand <- list) {
        rewrittenOpsRange(cand) = range
        val prevList = rewrittenRangeDefs.getOrElseUpdate(range, Nil)
        rewrittenRangeDefs(range) = prevList :+ (cand -> candidatesNode(cand))
      }
    }
  }

  val rewrittenOpsRange = scala.collection.mutable.Map[Rep[Any], Rep[Any]]()
  val rewrittenRangeDefs = scala.collection.mutable.Map[Rep[Any], List[(Rep[Any], Def[Any])]]()
  val alreadyRewrittenRange = scala.collection.mutable.Set[Rep[Any]]()
  val alreadyRewrittenOpsVars = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  /**
   * Specifies that the given loop symbol should be fused horizontally.
   * In this case one of the fused loops is visited for the first time.
   * So, the loop fusion should be also performed here.
   */
  def shouldBeFused(sym: Rep[Any]): Boolean =
    rewrittenOpsRange.get(sym) match {
      case Some(range) => !alreadyRewrittenRange.contains(range)
      case None        => false
    }

  /**
   * Specifies that the given loop symbol should be fused horizontally.
   * In this case one of the fused loops was already visited.
   * So, no loop fusion should be also performed here. Instead, the computed
   * value from the fused loop should be extracted and reused here.
   */
  def shouldBeReused(sym: Rep[Any]): Boolean =
    rewrittenOpsRange.get(sym) match {
      case Some(range) => alreadyRewrittenRange.contains(range)
      case None        => false
    }

  // TODO move to SC?
  /**
   * Applies CSE while reifying the statements if possible.
   */
  override def transformStmWithNewSym(stm: Stm[_]): to.Stm[_] = stm match {
    case Stm(sym, rhs) =>
      val newdef = transformDef(rhs)(sym.tp)
      to.IRReifier.findSymbol(newdef)(newdef.tp) match {
        case Some(existingSym) =>
          subst += sym -> existingSym
          to.Stm(existingSym, newdef)(existingSym.tp)
        case None =>
          val transformedSym = to.fresh(transformType(sym.tp))
          subst += sym -> transformedSym

          val stmt = to.Stm(transformedSym, newdef)(transformedSym.tp)
          to.reflectStm(stmt)
          stmt
      }
  }

  // TODO generalize, currently it works on very limited cases
  /**
   * Checks if the two given lambda functions are alpha-equivalent.
   */
  def sameFunctions[T1, T2, T3](f1: Lambda2[T1, T2, T3], f2: Lambda2[T1, T2, T3]): Boolean = {
    val (body1, body2) = f1.body -> f2.body
    if (body1.stmts.size == body2.stmts.size) {
      val context = collection.mutable.Map.empty[Rep[Any], Rep[Any]]
      context ++= f1.inputs.zip(f2.inputs)
      def transformSym(funArg: PardisFunArg): PardisFunArg = funArg match {
        case sym: Sym[_] => context.get(sym) match {
          case Some(s) => s
          case None    => funArg
        }
        case _ => funArg
      }
      body1.stmts.zip(body2.stmts).forall({
        case (stm1, stm2) =>
          val result = stm1.rhs.rebuild(stm1.rhs.funArgs.map(transformSym): _*) == stm2.rhs
          context += stm1.sym -> stm2.sym
          result
      })
    } else {
      false
    }
  }

  rewrite += statement {
    case sym -> (node @ QueryFoldLeft(range, _, _)) if shouldBeFused(sym) =>
      alreadyRewrittenRange += range.asInstanceOf[Rep[Any]]
      val nodes = rewrittenRangeDefs(range).map(_._2)
      for (node <- nodes) {
        assert(node.isInstanceOf[QueryFoldLeft[_, _]])
      }
      val zeros = for (QueryFoldLeft(_, z, _) <- nodes) yield z
      val functions = for (QueryFoldLeft(_, _, f) <- nodes) yield f
      val symbols = for (r <- rewrittenRangeDefs(range)) yield r._1
      /**
       * Keeps all the information about a foldLeft loop
       */
      case class Element(f: Rep[(Any, Any) => Any], zero: Rep[Any], symbol: Rep[Any], index: Int) {
        def func: Lambda2[Any, Any, Any] = f.correspondingNode.asInstanceOf[Lambda2[Any, Any, Any]]
      }
      // Finds the loops which are performing the same task and creates
      // the list of unique loops. Also, creates a list of indices for 
      // the other loops to refer to the index of the corresponding loop
      // in the unique list of loops. 
      val (uniqueLoops, indices) = {
        val uniqueList = scala.collection.mutable.ArrayBuffer[Element]()
        val indicesList = scala.collection.mutable.ArrayBuffer[Int]()
        for ((((f, z), s), i) <- functions.zip(zeros).zip(symbols).zipWithIndex) {
          val func = f.correspondingNode.asInstanceOf[Lambda2[Any, Any, Any]]
          uniqueList.find(e => sameFunctions(e.func, func) && e.zero == z) match {
            case Some(x) => indicesList += x.index
            case None =>
              indicesList += uniqueList.size
              uniqueList += Element(f, z, s, uniqueList.size)
          }

        }
        (uniqueList.toList, indicesList.toList)
      }
      // Generates the definition of variables which are mutated in the fold
      // loops
      val variables = uniqueLoops.map(loop => {
        val z = loop.zero
        __newVar(z)(z.tp)
      })

      // Based on the unique list of loops and the associated index for other loops,
      // specifies that each fused loop should use which accumulating variable.
      for ((index, sym) <- indices.zip(symbols)) {
        val v = variables(index)
        alreadyRewrittenOpsVars(sym) = v
      }

      class T
      implicit val typeT = range.tp.typeArguments(0).asInstanceOf[TypeRep[T]]

      // Rewrites all the horizontal loops into a single loop.
      range.asInstanceOf[Rep[Query[T]]].foreach {
        __lambda { i =>
          uniqueLoops.zip(variables).foreach({
            case (loop, v) =>
              val f = loop.f
              val varTp = v.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
              __assign(v, inlineFunction(f, readVar(v)(varTp), i))
          })
          unit()
        }
      }

      val variable = alreadyRewrittenOpsVars(sym)
      readVar(variable)(variable.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  /*
   * This rewrite rule is triggered whenever a loop which should be fused horizontally
   * was already rewritten. As a result now only the result from the fused loop should
   * be extracted and reused.
   */
  rewrite += statement {
    case sym -> (node @ QueryFoldLeft(range, _, _)) if shouldBeReused(sym) =>
      val variable = alreadyRewrittenOpsVars(sym)
      readVar(variable)(variable.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }
}
