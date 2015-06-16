package ch.epfl.data
package dblab.legobase
package compiler

import prettyprinter._
import sc.pardis.ir._
import sc.pardis.optimization._
import sc.pardis.types._

/**
 * Factory for creating a transformation phase which does not change the given program, but dumps its representation into a file.
 * The main use case is for debugging the transformation phases in a transformation pipeline.
 */
object TreeDumper {
  /**
   * Creates a tree dumper transformation phase.
   *
   * @param concreteSyntax specifies if the dumped tree should be printed in the concrete syntax form  or in the IR form
   */
  def apply(concreteSyntax: Boolean) = new TransformerHandler {
    def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
      if (concreteSyntax) {
        val cg = new LegoScalaGenerator(false, "tree_debug_dump.txt", "DummyRunner")
        val pw = new java.io.PrintWriter("tree_debug_dump.txt")
        val doc = cg.blockToDocument(block)
        doc.format(40, pw)
        pw.flush()
      } else {
        val pw = new java.io.PrintWriter(new java.io.File("tree_debug_dump.txt"))
        pw.println(block.toString)
        pw.flush()
      }

      block
    }
  }
}
