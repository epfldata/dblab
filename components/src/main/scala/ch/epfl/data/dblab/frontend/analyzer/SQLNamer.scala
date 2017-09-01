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

  var curSchema = schema

  def findRelationName(attr: String): String = {
    def filterTables(tables: List[Table]): List[String] = tables.filter(t => t.findAttribute(attr).nonEmpty).map(_.name).distinct
    val relations = filterTables(curSchema.tables.toList)
    val filteredRelations = if (relations.size > 1) relations diff filterTables(schema.tables.toList) else relations
    filteredRelations match {
      case List(rel) => rel
      case Nil       => throw new Exception(s"No relation found with attribute $attr. Schema: $curSchema")
      case rels      => throw new Exception(s"Multiple relations found with attribute $attr: ${rels.mkString(", ")}")
    }
  }

  def nameExprOptional(forced: Boolean)(exp: Expression): Expression = {
    exp match {
      case tl: TopLevelStatement           => nameQuery(tl)
      case ExpressionShape(fact, children) => fact(children.map(nameExprOptional(forced)))
      case FieldIdent(None, a, s) =>
        val qualifier = try {
          Some(findRelationName(a))
        } catch {
          case ex: Exception if !forced => None
        }
        FieldIdent(qualifier, a, s)
      case _ => exp
    }
  }

  def nameExpr(exp: Expression): Expression = nameExprOptional(true)(exp)

  def extractSources(rel: Relation): List[Relation] = rel match {
    case SQLTable(_, _)          => List(rel)
    case Subquery(_, _)          => List(rel)
    case Join(left, right, _, _) => extractSources(left) ++ extractSources(right)
    case _                       => ???
  }

  // TODO maybe add the type!
  type TableSchema = List[String]
  type LabeledSchema = (TableSchema, String)

  def getSourceLabeledSchema(rel: Relation): LabeledSchema = rel match {
    case SQLTable(n, a) => curSchema.findTable(n) match {
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

  def labeledSchemaToTable(ls: LabeledSchema): Table = {
    val name = ls._2
    val attrs = ls._1.map(n => Attribute(n, AnyType))
    Table(name, attrs, collection.mutable.ArrayBuffer(), "")
  }

  def nameSource(rel: Relation): Relation = rel match {
    case SQLTable(name, alias) => SQLTable(name, Some(alias.getOrElse(name)))
    case Subquery(e, a)        => Subquery(nameQuery(e), a)
    case Join(left, right, kind, clause) => {
      val nl = nameSource(left)
      val nr = nameSource(right)
      val (nk, nc) = kind match {
        case NaturalJoin =>
          val nls = getSourceLabeledSchema(nl)
          val nrs = getSourceLabeledSchema(nr)
          def getCommon(l1: List[String], l2: List[String]): Option[String] =
            l1.find(x => l2.contains(x))
          val commonAttr = getCommon(nls._1, nrs._1)
          val eqClause = commonAttr.map(c => Equals(FieldIdent(Some(nls._2), c), FieldIdent(Some(nrs._2), c)))
          InnerJoin -> eqClause.getOrElse(null)
        case _ =>
          kind -> clause
      }
      Join(nl, nr, nk, nc)
    }
    case _ => ???
  }

  def withSchema[T](newSchema: Schema)(f: () => T): T = {
    val oldSchema = curSchema
    curSchema = newSchema
    val res = f()
    curSchema = oldSchema
    res
  }

  def extractJoinEqualities(rel: Relation): List[Equals] = rel match {
    case SQLTable(_, _) => Nil
    case Subquery(_, _) => Nil
    case Join(left, right, kind, clause) =>
      val currentEqClause = clause match {
        case null                                 => Nil
        case Equals(IntLiteral(1), IntLiteral(1)) => Nil // already handlered by the parser
        case e: Equals                            => List(e)
        case And(e1: Equals, e2: Equals)          => List(e1, e2)
      }
      currentEqClause ++ extractJoinEqualities(left) ++ extractJoinEqualities(right)
    case _ => Nil
  }

  def nameSelect(select: SelectStatement): SelectStatement = {
    select match {
      case SelectStatement(withs, projections: ExpressionProjections, source, where, groupBy, having, orderBy, limit, aliases) =>
        val namedSource = source.map(nameSource)
        val rels = namedSource.map(extractSources).getOrElse(Seq()).toList
        val namedRels = rels.map({
          case Subquery(e, a) => Subquery(nameQuery(e), a)
          case rel            => rel
        })
        val labeledRels = namedRels.map(getSourceLabeledSchema)
        val newSchema = curSchema.copy(tables = curSchema.tables ++ labeledRels.map(labeledSchemaToTable))
        val namedProjections = withSchema(newSchema)(() => {
          projections.lst.flatMap(exp => exp match {
            case (StarExpression(source), None) =>
              val filteredLabeledRels = source match {
                case None => labeledRels
                case Some(rel) => labeledRels.find(_._2 == rel) match {
                  case Some(v) => List(v)
                  case None    => throw new Exception(s"Could not find a reference to relation $rel")
                }
              }
              filteredLabeledRels.flatMap {
                case (table, name) =>
                  table.map(a => FieldIdent(Some(name), a) -> Some(a))
              }
            case (FieldIdent(q, a, s), alias) =>
              val nq = q.getOrElse(findRelationName(a))
              val newAlias = alias.getOrElse(a)
              List(FieldIdent(Some(nq), a, s) -> Some(newAlias))
            case (e, None) =>
              List(nameExpr(e) -> Some(newVarName("var")))
            case (e, Some(a)) => List(nameExpr(e) -> Some(a))
          })
        })
        val namedWhere = withSchema(newSchema)(() => where.map(nameExpr))
        val joinEqs = namedSource.map(ns => extractJoinEqualities(ns).map(nameExpr)).getOrElse(Nil)
        val newWhere = namedWhere match {
          case Some(v) => Some(joinEqs.foldLeft(v)((x, y) => And(x, y)))
          case None => joinEqs match {
            case hd :: tl => Some(tl.foldLeft[Expression](hd)((x, y) => And(x, y)))
            case Nil      => None
          }
        }
        val namedGroupBy = withSchema(newSchema)(() => groupBy.map(gb => gb.copy(keys = gb.keys.map(nameExprOptional(false)))))
        val namedOrderBy = withSchema(newSchema)(() => orderBy.map(ob => ob.copy(keys = ob.keys.map(x => nameExprOptional(false)(x._1) -> x._2))))
        SelectStatement(withs, ExpressionProjections(namedProjections), namedSource, newWhere, namedGroupBy, having, namedOrderBy, limit, aliases)
    }
  }
}
