package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.CalcAST._
import parser.SQLAST._
import sc.pardis.ast._
import sc.pardis.rules._

trait FixedPointTransformer extends Transformer {
  def maxIterations: Int = 10
  def apply(node: ThisNode): ThisNode = transformToZero(node, maxIterations)
  def transformToZero(node: ThisNode, iters: Int): ThisNode = {
    val tnode = transform(node)
    // println(tnode)
    if (node == tnode || iters <= 1) // substitute by a correct equality check
      tnode
    else
      transformToZero(tnode, iters - 1)
  }
}

trait RecursiveTransformer extends Transformer {
  def definedRules: List[Rule]

  object ApplicableRule {
    def unapply(node: Node): Option[Rule] =
      definedRules.find(r => r.canGenerate(node))
  }

  def pprint(node: ThisNode): String
}

trait TopDownRecursiveTransformer extends RecursiveTransformer {
  abstract override def transform(node: ThisNode): ThisNode = node match {
    case ApplicableRule(rule) => {
      val nnode = rule.generate(node).get.asInstanceOf[ThisNode]
      //      println(s"rule $rule applicable to\n>>${pprint(node)}\n<<${pprint(nnode)}")
      super.transform(nnode)
    }
    case _ => super.transform(node)
  }
}

trait BottomUpRecursiveTransformer extends RecursiveTransformer {

  abstract override def transform(node: ThisNode): ThisNode = super.transform(node) match {
    case x @ ApplicableRule(rule) => {
      val nnode = rule.generate(x).get.asInstanceOf[ThisNode]
      //      println(s"rule $rule applicable to\n>>${pprint(x)}\n<<${pprint(nnode)}")
      nnode

    }
    case x => x
  }
}

class CalcRuleBasedTransformer(rules: List[Rule])
  extends CalcTransformer
  with BottomUpRecursiveTransformer
  with FixedPointTransformer {
  def definedRules = rules

  def pprint(node: ThisNode): String = prettyprint(node)
}
