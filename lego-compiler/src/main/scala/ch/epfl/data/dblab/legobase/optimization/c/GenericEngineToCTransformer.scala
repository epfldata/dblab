package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._

class GenericEngineToCTransformer(override val IR: LoweringLegoBase, val settings: compiler.Settings) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  val initTimer = infix_asInstanceOf[TimeVal](unit(0))

  case class Timer(diff: Rep[TimeVal], start: Rep[TimeVal], end: Rep[TimeVal])
  def createTimeVal: Rep[TimeVal] = readVar(__newVar(initTimer))
  def createTimer: Timer = Timer(createTimeVal, createTimeVal, createTimeVal)

  var timer: Timer = _

  override def optimize[T: TypeRep](node: Block[T]): Block[T] = {
    traverseBlock(node)
    if (settings.onlyLoading) {
      reifyBlock {
        timer = createTimer
        gettimeofday(&(timer.start))
        inlineBlock(transformProgram(node))
      }
    } else {
      transformProgram(node)
    }
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      if (settings.onlyLoading) {
        val Timer(diff, start, end) = timer
        // Don't put the query processing!
        gettimeofday(&(end))
        val tm = timeval_subtract(&(diff), &(end), &(start))
        printf(unit("*****ONLY LOADING*****\nGenerated code run in %ld milliseconds.\n"), tm)
      } else {
        val Timer(diff, start, end) = createTimer
        gettimeofday(&(start))
        inlineBlock(apply(b))
        gettimeofday(&(end))
        val tm = timeval_subtract(&(diff), &(end), &(start))
        printf(unit("Generated code run in %ld milliseconds.\n"), tm)
      }

  }
  rewrite += rule { case GenericEngineParseStringObject(s) => s }
  rewrite += rule {
    case GenericEngineDateToStringObject(d) => NameAlias[String](None, "ltoa", List(List(d)))
  }
  rewrite += rule {
    case GenericEngineDateToYearObject(d) => d / unit(10000)
  }
  rewrite += rule {
    case GenericEngineParseDateObject(Constant(d)) =>
      val data = d.split("-").map(x => x.toInt)
      unit((data(0) * 10000) + (data(1) * 100) + data(2))
  }
}
