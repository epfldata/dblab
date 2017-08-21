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

  def extractSources(rel: Relation): List[Relation] = rel match {
    case SQLTable(_, _)          => List(rel)
    case Subquery(_, _)          => List(rel)
    case Join(left, right, _, _) => extractSources(left) ++ extractSources(right)
    case _                       => ???
  }

  // TODO maybe add the type!
  type TableSchema = List[String]

  def getSourceLabeledSchema(rel: Relation): (TableSchema, String) = rel match {
    case SQLTable(n, a) => schema.findTable(n).get.attributes.map(_.name) -> a.getOrElse(n)
    case Subquery(e, a) => e match {
      case UnionIntersectSequence(left, right, kind) => ???
      case st: SelectStatement                       => getSelectSchema(st) -> a
      case _                                         => ???
    }
    case _ => ???
  }

  def getSelectSchema(select: SelectStatement): TableSchema = {
    val projs = select.projections.asInstanceOf[ExpressionProjections].lst
    projs.map(_._2.get).toList
  }

  def nameSource(rel: Relation): Relation = rel match {
    case SQLTable(name, alias)           => SQLTable(name, Some(alias.getOrElse(name)))
    case Subquery(e, a)                  => Subquery(nameQuery(e), a)
    case Join(left, right, kind, clause) => Join(nameSource(left), nameSource(right), kind, clause)
    case _                               => ???
  }

  def nameSelect(select: SelectStatement): SelectStatement = {
    select match {
      case SelectStatement(withs, projections: ExpressionProjections, source, where, groupBy, having, orderBy, limit, aliases) =>
        val namedSource = source.map(nameSource)
        val namedProjections = projections.lst.flatMap(exp => exp match {
          case (StarExpression(source), None) =>
            val rels = namedSource.map(extractSources).getOrElse(Seq()).toList
            val namedRels = rels.map({
              case Subquery(e, a) => Subquery(nameQuery(e), a)
              case rel            => rel
            })
            val labeledRels = namedRels.map(getSourceLabeledSchema)
            val filteredLabeledRels = source match {
              case None      => labeledRels
              case Some(rel) => List(labeledRels.find(_._2 == rel).get) // intentionally used .get to give an error if the names don't match
            }
            filteredLabeledRels.flatMap {
              case (table, name) =>
                table.map(a => FieldIdent(Some(name), a) -> Some(a))
            }
          case _ => List(exp)
        })
        SelectStatement(withs, ExpressionProjections(namedProjections), namedSource, where, groupBy, having, orderBy, limit, aliases)
    }
  }
}
