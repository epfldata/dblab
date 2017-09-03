package ch.epfl.data
package dblab
package frontend
package analyzer

import sc.pardis.types._
import PardisTypeImplicits._
import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._
import parser.SQLAST._
import sc.pardis.shallow.OptimalString

/**
 * Type checking and inference for SQL expressions.
 *
 * @author Amir Shaikhha
 */
class SQLTyper(schema: Schema) {
  val namer = new SQLNamer(schema)
  var curSchema = schema
  def withSchema[T](newSchema: Schema)(f: () => T): T = {
    val oldSchema = curSchema
    curSchema = newSchema
    val res = f()
    curSchema = oldSchema
    res
  }
  def typeQuery(query: TopLevelStatement): TopLevelStatement = {
    query match {
      case UnionIntersectSequence(left, right, kind) =>
        UnionIntersectSequence(typeQuery(left), typeQuery(right), kind)
      case stmt: SelectStatement => typeSelect(stmt)
    }
  }

  def escalateType(a: Tpe, b: Tpe): Tpe = {
    val lattice = Map[Tpe, Int](BooleanType -> 0, IntType -> 1, FloatType -> 2, DoubleType -> 3)
    def isSubType(a: Tpe, b: Tpe): Boolean = (lattice.get(a), lattice.get(b)) match {
      case (Some(an), Some(bn)) => an < bn
      case _                    => false
    }
    (a, b) match {
      case (at, bt) if at == bt          => at
      case (at, bt) if isSubType(at, bt) => bt
      case (at, bt) if isSubType(bt, at) => at
      case _                             => throw new Exception(s"escalating types $a, $b"); AnyType
    }
  }

  def typeExpr(exp: Expression): Expression = {
    exp match {
      case tl: TopLevelStatement => typeQuery(tl)
      case ExpressionShape(_, children) =>
        children.foreach(typeExpr)
        val tpe = exp match {
          case Equals(_, _) | NotEquals(_, _) | LessOrEqual(_, _) |
            LessThan(_, _) | GreaterOrEqual(_, _) | GreaterThan(_, _) => BooleanType
          case Sum(_) | Min(_) | Max(_) | Avg(_) => children.head.tpe
          case CountExpr(_) | CountAll()         => IntType
          case Add(_, _) | Subtract(_, _) | Multiply(_, _) | Divide(_, _) =>
            val List(tpe1, tpe2) = children.map(_.tpe)
            escalateType(tpe1, tpe2)
          case And(_, _) | Or(_, _) =>
            assert(children.forall(_.tpe == BooleanType))
            BooleanType
          case DateLiteral(_) | IntLiteral(_) =>
            IntType
          case FloatLiteral(_) =>
            FloatType
          case DoubleLiteral(_) =>
            DoubleType
          case StringLiteral(_) =>
            StringType
          case CharLiteral(_) =>
            CharType
        }
        exp.tpe = tpe
        exp
      case FieldIdent(Some(q), a, s) =>
        val table = curSchema.findTable(q).getOrElse(throw new Exception(s"Couldn't find table $q"))
        val attr = table.findAttribute(a).getOrElse(throw new Exception(s"Couldn't find attr $a in table $q"))
        exp.tpe = attr.dataType match {
          case DateType => IntType
          case t        => t
        }
        println(s"inferred type for $exp: ${exp.tpe}")
        exp
      case FieldIdent(None, a, s) =>
        throw new Exception(s"$exp is without quantifier. You forgot to use the namer?")
      case _ => throw new Exception(s"Does not handle $exp (${exp.getClass}) yet")
    }
  }

  type TableSchema = List[(String, Tpe)]
  type LabeledSchema = (TableSchema, String)

  def getSourceLabeledSchema(rel: Relation): LabeledSchema = rel match {
    case SQLTable(n, a) => curSchema.findTable(n) match {
      case Some(t) => t.attributes.map(x => x.name -> x.dataType) -> a.getOrElse(n)
      case None    => throw new Exception(s"The schema doesn't have table `$n`")
    }
    case Subquery(e, a) => e match {
      case UnionIntersectSequence(left, right, kind) => ???
      case st: SelectStatement                       => getSelectSchema(st) -> a
      case _                                         => ???
    }
    case _ => ???
  }

  def labeledSchemaToTable(ls: LabeledSchema): Table = {
    val name = ls._2
    val attrs = ls._1.map(n => Attribute(n._1, n._2))
    Table(name, attrs, collection.mutable.ArrayBuffer(), "")
  }

  def getSelectSchema(select: SelectStatement): TableSchema = {
    val projs = select.projections.asInstanceOf[ExpressionProjections].lst
    projs.map(x => x._2.get -> AnyType).toList
  }

  def typeSource(rel: Relation): Relation = rel match {
    case SQLTable(name, alias) => rel
    case Subquery(e, a) =>
      typeQuery(e); rel
    case Join(left, right, kind, clause) => {
      typeSource(left)
      typeSource(right)
      rel
    }
    case _ => ???
  }

  def typeSelect(select: SelectStatement): SelectStatement = {
    select match {
      case SelectStatement(withs, projections: ExpressionProjections, source, where, groupBy, having, orderBy, limit, aliases) =>
        source.foreach(typeSource)
        val rels = source.map(namer.extractSources).getOrElse(Seq()).toList
        val labeledRels = rels.map(getSourceLabeledSchema)
        val newSchema = curSchema.copy(tables = curSchema.tables ++ labeledRels.map(labeledSchemaToTable))
        withSchema(newSchema) { () =>
          projections.lst.foreach(x => typeExpr(x._1))
          where.foreach(typeExpr)
          groupBy.foreach(_.keys.foreach(typeExpr))
          orderBy.foreach(_.keys.foreach(x => typeExpr(x._1)))
        }
        select
    }
  }
}
