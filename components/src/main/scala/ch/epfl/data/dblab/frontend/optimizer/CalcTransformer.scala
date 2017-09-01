package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.CalcAST._
import sc.pardis.ast._

/**
 * @author Amir Shaikhha
 */

trait CalcTransformer extends Transformer {
  type ThisNode = CalcExpr
  def transform(node: CalcExpr): CalcExpr = {
    val (fact, children) = CalcExprShape.unapply(node).get
    val result = fact(children.map(transform))
    updateSymbol(node, result)
  }
  def updateSymbol(oldNode: CalcExpr, newNode: CalcExpr): CalcExpr = {
    if (oldNode.symbol != null) {
      val oldProps = oldNode.symbol.properties
      val newSymbol = NodeSymbol(newNode)
      for ((pf, p) <- oldProps if pf.constant) {
        newSymbol.updateProperty(p)
      }
      newNode.symbol = newSymbol
    }
    newNode
  }
}
