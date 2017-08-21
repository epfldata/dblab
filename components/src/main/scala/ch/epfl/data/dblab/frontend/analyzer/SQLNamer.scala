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
 * Performs naming on SQL queries, in order to handle aliasing, substituting
 * the asterisk (*) with the actual list of column names, and making field
 * identifiers fully qualified.
 *
 * @author Amir Shaikhha
 */
class SQLNamer(schema: Schema) {
  // TODO reuse the existing ones from SC.
  var globalId = 0
  def newVarName(prefix: String): String = {
    globalId += 1
    s"$prefix$globalId"
  }

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
    case SQLTable(n, a) => schema.findTable(n) match {
      case Some(t) => t.attributes.map(_.name) -> a.getOrElse(n)
      case None    => throw new Exception(s"The schema doesn't have table `$n`")
    }
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
              case None => labeledRels
              case Some(rel) => labeledRels.find(_._2 == rel) match {
                case Some(v) => List(v)
                case None    => throw new Exception(s"Could not find a reference to relation $rel")
              } // intentionally used .get to give an error if the names don't match
            }
            filteredLabeledRels.flatMap {
              case (table, name) =>
                table.map(a => FieldIdent(Some(name), a) -> Some(a))
            }
          case (FieldIdent(q, a, s), None) =>
            List(FieldIdent(q, a, s) -> Some(a))
          case (e, None) =>
            List(e -> Some(newVarName("var")))
          case _ => List(exp)
        })
        SelectStatement(withs, ExpressionProjections(namedProjections), namedSource, where, groupBy, having, orderBy, limit, aliases)
    }
  }
}
