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

  def shouldBeFused(sym: Rep[Any]): Boolean =
    rewrittenOpsRange.get(sym) match {
      case Some(range) => !alreadyRewrittenRange.contains(range)
      case None        => false
    }

  def shouldBeReused(sym: Rep[Any]): Boolean =
    rewrittenOpsRange.get(sym) match {
      case Some(range) => alreadyRewrittenRange.contains(range)
      case None        => false
    }

  rewrite += statement {
    case sym -> (node @ QueryFoldLeft(range, _, _)) if shouldBeReused(sym) =>
      val variable = alreadyRewrittenOpsVars(sym)
      readVar(variable)(variable.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  override def transformStmWithNewSym(stm: Stm[_]): to.Stm[_] = stm match {
    case Stm(sym, rhs) =>
      val newdef = transformDef(rhs)(sym.tp)
      to.findSymbol(newdef)(newdef.tp) match {
        case Some(existingSym) =>
          subst += sym -> existingSym
          to.Stm(existingSym, newdef)(existingSym.tp)
        case None =>
          val transformedSym = to.fresh(transformType(sym.tp))
          subst += sym -> transformedSym

          val stmt = to.Stm(transformedSym, newdef)(transformedSym.tp)
          // maybe here we can apply CSE
          to.reflectStm(stmt)
          stmt
      }
  }

  // TODO generalize, currently it works on very limited cases
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
      // System.out.println(s"$range rewritten")
      // System.out.println(s"${rewrittenRangeDefs(range).size} is the size")
      // System.out.println(s"functions: ${functions.map(_.correspondingNode).mkString("\n")}")
      // val (f0, f2) = {
      //   val defs = functions.map(_.correspondingNode).asInstanceOf[List[Lambda2[Any, Any, Any]]]
      //   defs(0) -> defs(2)
      // }
      // System.out.println(s"f0: $f0 \nf2: $f2\n${sameFunctions(f0, f2)}")
      case class Element(f: Rep[(Any, Any) => Any], zero: Rep[Any], symbol: Rep[Any], index: Int) {
        def func: Lambda2[Any, Any, Any] = f.correspondingNode.asInstanceOf[Lambda2[Any, Any, Any]]
      }
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
      System.out.println(s"uniqueList: ${uniqueLoops.mkString("\n")}, indices: $indices")
      val variables = uniqueLoops.map(loop => {
        // System.out.println(s"$z.tp: ${z.tp}")
        val z = loop.zero
        __newVar(z)(z.tp)
      })
      // for ((v, sym) <- variables.zip(symbols)) {
      //   alreadyRewrittenOpsVars(sym) = v
      // }

      for ((index, sym) <- indices.zip(symbols)) {
        val v = variables(index)
        alreadyRewrittenOpsVars(sym) = v
      }
      range.foreach {
        __lambda { i =>
          uniqueLoops.zip(variables).foreach({
            case (loop, v) =>
              val f = loop.f
              val varTp = v.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
              // System.out.println(s"v.tp: ${varTp}")
              __assign(v, inlineFunction(f, readVar(v)(varTp), i))
          })
          unit()
        }
      }

      val variable = alreadyRewrittenOpsVars(sym)
      readVar(variable)(variable.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])

    // val allFused = range.foldLeft(Array(zeros: _*))(__lambda { (acc, cur) =>
    //   Array(functions.zipWithIndex.map({
    //     case (f, index) =>
    //       inlineFunction(f, acc(unit(index)), cur)
    //   }): _*)
    // })
    // allFused(unit(0))

    // System.out.println(s"currentScope: ${currentScope.toString.take(24)}")
    // candidates += sym
    // candidatesNode(sym) = node
    // candidatesScope(sym) = currentScope
    // sym
  }
}
