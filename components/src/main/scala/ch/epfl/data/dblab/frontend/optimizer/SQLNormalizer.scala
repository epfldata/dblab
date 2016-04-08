package ch.epfl.data
package dblab
package frontend
package optimizer

import parser._

/**
 * The general interface each SQL normalizer of DBLAB must abide to.
 * Observe that since the main normalize function takes as input and returns
 * an SQLAST, this allows normalizers to be chained together.
 *
 * @author Yannis Klonatos
 */
trait SQLNormalizer {
  def normalize(node: TopLevelStatement): TopLevelStatement = node match {
    case UnionIntersectSequence(top, bottom, connectionType) => UnionIntersectSequence(normalize(top), normalize(bottom), connectionType)
    case stmt: SelectStatement                               => normalizeStmt(stmt)
  }
  def normalizeStmt(stmt: SelectStatement): SelectStatement;
}
