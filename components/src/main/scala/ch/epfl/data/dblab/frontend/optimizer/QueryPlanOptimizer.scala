package ch.epfl.data
package dblab
package frontend
package optimizer

import parser.OperatorAST._

/**
 * The general interface each query optimizer of DBLAB must abide to.
 * Observe that since the main optimize function takes as input and returns
 * an operator tree, this allows optimizers to be chained together.
 *
 * @author Yannis Klonatos
 */
trait QueryPlanOptimizer {
  def optimize(tree: QueryPlanTree): QueryPlanTree
}
