package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.SQLAST.{ ExternalFunctionExp, _ }

/**
 * Created by mohsen on 9/1/17.
 */

object SQLUtils {

  def is_agg_expr(expr: Expression): Boolean = {
    // There is a isAggregateOpExpr in scala but seems different
    println(expr)
    expr match {
      case _: LiteralExpression              => false
      case _: FieldIdent                     => false
      case b: BinaryOperator                 => is_agg_expr(b.left) || is_agg_expr(b.right)
      case u: UnaryOperator                  => is_agg_expr(u.expr)
      case _: Aggregation                    => true
      case ExternalFunctionExp(name, inputs) => inputs.map(x => is_agg_expr(x)).foldLeft(false)((sum, cur) => sum || cur)
      case s: SelectStatement                => false
      case d: Distinct                       => false // its not in ocaml
      //TODO nested_q , cases in Ocaml and others in Scala
    }
  }

  def Inverse(e: BinaryOperator): BinaryOperator = {
    val e1 = e.left
    val e2 = e.right
    e match {
      case _: Equals         => NotEquals(e1, e2)
      case _: NotEquals      => Equals(e1, e2)
      case _: GreaterThan    => LessOrEqual(e1, e2)
      case _: LessOrEqual    => GreaterThan(e1, e2)
      case _: GreaterOrEqual => LessThan(e1, e2)
      case _: LessThan       => GreaterOrEqual(e1, e2)

    }
  }
}
