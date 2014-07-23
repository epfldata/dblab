package ch.epfl.data
package legobase
package deep

import pardis.ir._

abstract class Optimizer[Lang <: Base](val IR: Lang) extends TopDownTransformerTraverser[Lang] {
  import IR._
  def optimize[T: Manifest](node: Block[T]): Block[T]
}
