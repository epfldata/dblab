package ch.epfl.data
package dblab
package frontend
package analyzer

import sc.pardis.types._
import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._
import parser.SQLAST._
import sc.pardis.shallow.OptimalString

/**
 * Performs naming on SQL queries, in order to handle aliasing and substituting
 * the asterisk (*) with the actual list of column names.
 *
 * @author Yannis Klonatos
 */
class SQLNamer(schema: Schema) {

  def nameQuery(query: TopLevelStatement): TopLevelStatement = {
    query match {
      case UnionIntersectSequence(left, right, kind) =>
        UnionIntersectSequence(nameQuery(left), nameQuery(right), kind)
      case stmt: SelectStatement => nameSelect(stmt)
    }
  }

  def nameSelect(select: SelectStatement): SelectStatement = {
    select
  }
}
