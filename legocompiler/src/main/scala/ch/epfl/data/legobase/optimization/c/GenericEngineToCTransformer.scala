package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._

class GenericEngineToCTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  rewrite += rule {
    case GenericEngineRunQueryObject(b) =>
      val init = infix_asInstanceOf[TimeVal](unit(0))
      val diff = readVar(__newVar(init))
      val start = readVar(__newVar(init))
      val end = readVar(__newVar(init))
      gettimeofday(&(start))
      inlineBlock(apply(b))
      gettimeofday(&(end))
      val tm = timeval_subtract(&(diff), &(end), &(start))
      printf(unit("Generated code run in %ld milliseconds.\n"), tm)
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
