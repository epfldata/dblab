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
    select match {
      case SelectStatement(withs, projections: ExpressionProjections, joinTree, where, groupBy, having, orderBy, limit, aliases) =>
        val namedProjections = projections.lst.flatMap(exp => exp match {
          case (StarExpression(source), None) =>
            val rels = joinTree.map(_.extractRelations).getOrElse(Seq()).toList
            val namedRels = rels.map(rel => rel match {
              case SQLTable(n, a) => n -> a.getOrElse(n)
              case Subquery(_, a) => ???
              case _              => ???
            })
            val filteredNamedRels = source match {
              case None      => namedRels
              case Some(rel) => List(namedRels.find(_._2 == rel).get) // intentionally use get to give error if the names don't match
            }
            // println(s"None * $filteredNamedRels $namedRels")
            val aliasedTables = filteredNamedRels.flatMap(x => schema.findTable(x._1).map(t => t -> x._2))
            assert(aliasedTables.length == filteredNamedRels.length)
            aliasedTables.flatMap {
              case (table, name) =>
                table.attributes.map(a => FieldIdent(Some(name), a.name) -> Some(a.name))
            }
          case _ => List(exp)
        })
        SelectStatement(withs, ExpressionProjections(namedProjections), joinTree, where, groupBy, having, orderBy, limit, aliases)
    }
  }
}
