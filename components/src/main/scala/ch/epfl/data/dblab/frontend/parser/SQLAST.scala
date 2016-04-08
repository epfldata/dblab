package ch.epfl.data
package dblab
package frontend
package parser

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * AST for SQL select statement.
 * Based on: https://github.com/stephentu/scala-sql-parser
 *
 * @author Yannis Klonatos
 */

trait Node

trait TopLevelStatement extends Node with Expression {
  def extractRelations(): Seq[Relation] = this match {
    case stmt: SelectStatement =>
      stmt.joinTree match {
        case Some(tree) => tree.extractRelations
        case None       => Seq()
      }
    case UnionIntersectSequence(top, bottom, _) => top.extractRelations ++ bottom.extractRelations
  }

  def extractSubqueries(): Seq[Subquery] = this match {
    case stmt: SelectStatement =>
      stmt.joinTree match {
        case Some(tree) => tree.extractSubqueries
        case None       => Seq()
      }
    case UnionIntersectSequence(top, bottom, _) => top.extractSubqueries ++ bottom.extractSubqueries
  }

  def extractAliases: Seq[(Expression, String, Int)] = this match {
    case stmt: SelectStatement                  => stmt.projections.extractAliases
    case UnionIntersectSequence(top, bottom, _) => top.extractAliases ++ bottom.extractAliases
  }

  def findProjection(e: Expression, alias: String): Seq[Expression] = this match {
    case stmt: SelectStatement                  => stmt.projections.findProjection(e, alias)
    case UnionIntersectSequence(top, bottom, _) => top.findProjection(e, alias) ++ bottom.findProjection(e, alias)
  }
}

object Identation {
  var indentationCounter = -1
  def printIdent = "\t" * indentationCounter
}

case class SelectStatement(withs: Seq[View],
                           projections: Projections,
                           joinTree: Option[Relation],
                           where: Option[Expression],
                           groupBy: Option[GroupBy],
                           having: Option[Having],
                           orderBy: Option[OrderBy],
                           limit: Option[Limit],
                           aliases: Seq[(Expression, String, Int)]) extends TopLevelStatement {
  import Identation._
  override def toString() = {
    indentationCounter += 1
    val descr =
      { if (indentationCounter == 0) printIdent + "SQL TREE" else "" } +
        "\n" + printIdent + "====================" +
        "\n" + printIdent + "WITHS            :" + {
          if (withs.size != 0) "\n" + withs.map(w => printIdent + "\t" + w.alias + w.subquery).mkString("\n")
          else ""
        } +
        "\n" + printIdent + "PROJECTIONS      :" + projections.toString +
        "\n" + printIdent + "RELATIONS        :" + this.extractRelations.mkString(",") +
        "\n" + printIdent + "JOINTREE         :" + joinTree.getOrElse("").toString +
        "\n" + printIdent + "WHERE CLAUSES    :" + where.getOrElse("").toString +
        "\n" + printIdent + "GROUP BY CLAUSES :" + groupBy.getOrElse("").toString +
        "\n" + printIdent + "HAVING           :" + having.getOrElse("").toString +
        "\n" + printIdent + "ORDER BY CLAUSES :" + orderBy.getOrElse("").toString +
        "\n" + printIdent + "LIMIT CLAUSE     :" + limit.getOrElse("").toString +
        "\n" + printIdent + "ALIASES          :" + aliases.map(al => al._1 + " -> " + al._2).mkString(" , ") +
        "\n" + printIdent + "====================" +
        { if (indentationCounter == 0) "\n" else "" }
    indentationCounter -= 1
    descr
  }
}
case class UnionIntersectSequence(top: TopLevelStatement, bottom: TopLevelStatement, connectionType: QueryRelationType) extends TopLevelStatement
sealed trait QueryRelationType
case object UNIONALL extends QueryRelationType
case object UNION extends QueryRelationType
case object INTERSECT extends QueryRelationType
case object SEQUENCE extends QueryRelationType
case object EXCEPT extends QueryRelationType

trait Projections extends Node {
  def size(): Int
  def get(n: Int): (Expression, Option[String])
  def extractAliases(): Seq[(Expression, String, Int)]
  def findProjection(e: Expression, alias: String): Seq[Expression]
}
case class ExpressionProjections(lst: Seq[(Expression, Option[String])]) extends Projections {
  def size(): Int = lst.size
  def get(n: Int) = lst(n)
  def findProjection(e: Expression, alias: String) = {
    // First attempt, look through all aliases for the one given
    lst.filter(l => l._2.isDefined).find(l => l._2.get == alias) match {
      case Some(p) => Seq(p._1)
      case None =>
        // Second attempt, go through the expression given and try to match it
        e match {
          case FieldIdent(id, name, sym) =>
            val flds = lst.filter(l => l._1.isInstanceOf[FieldIdent]).map(_._1.asInstanceOf[FieldIdent])
            val aggs = lst.filter(l => l._1.isInstanceOf[Aggregation])
            val fld = flds.find(f => f.name == name)
            val agg = aggs.find(ag => ag._2.get == name)
            if (fld.isDefined) Seq(fld.get)
            else if (agg.isDefined) Seq(agg.get._1)
            else Seq()
        }
    }
  }

  def getNames = lst.map(p => p._1 match {
    case fld: FieldIdent => p._2 match {
      case Some(al) => al
      case None     => fld.qualifier.getOrElse("") + fld.name
    }
    case c if c.isAggregateOpExpr => p._2.get
  })
  def extractAliases(): Seq[(Expression, String, Int)] = lst.zipWithIndex.filter(p => p._1._2.isDefined).map(al => (al._1._1, al._1._2.get, al._2))
}
case class AllColumns() extends Projections {
  def size(): Int = ???
  def get(n: Int) = ???
  def extractAliases(): Seq[(Expression, String, Int)] = Seq()
  def findProjection(e: Expression, alias: String) = ???
}

// Expressions
trait Expression extends Node {
  private var tpe: TypeTag[_] = null
  val isAggregateOpExpr = true
  def tp = tpe match {
    case t if t == null => tpe // TODO -- Introduce check here?
    case _              => tpe
  }
  def setTp[A](tt: TypeTag[A]) {
    if (tt == null && this != NullLiteral)
      throw new Exception("SQL Type Inferrence BUG: type of Expression " + this + " cannot be set to null.")
    this.tpe = tt
  }
}

trait BinaryOperator extends Expression {
  val left: Expression
  val right: Expression
}
case class Or(left: Expression, right: Expression) extends BinaryOperator
case class And(left: Expression, right: Expression) extends BinaryOperator

trait EqualityOperator extends BinaryOperator
case class Equals(left: Expression, right: Expression) extends EqualityOperator
case class NotEquals(left: Expression, right: Expression) extends EqualityOperator

trait InEqualityOperator extends BinaryOperator
case class LessOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class LessThan(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterThan(left: Expression, right: Expression) extends InEqualityOperator

case class Like(left: Expression, right: Expression) extends BinaryOperator
case class Add(left: Expression, right: Expression) extends BinaryOperator
case class Subtract(left: Expression, right: Expression) extends BinaryOperator
case class Multiply(left: Expression, right: Expression) extends BinaryOperator
case class Divide(left: Expression, right: Expression) extends BinaryOperator
case class StringConcat(left: Expression, right: Expression) extends BinaryOperator {
  override val isAggregateOpExpr = false
}

trait UnaryOperator extends Expression {
  val expr: Expression
}
case class Not(expr: Expression) extends UnaryOperator
case class Abs(expr: Expression) extends UnaryOperator
case class UnaryPlus(expr: Expression) extends UnaryOperator
case class UnaryMinus(expr: Expression) extends UnaryOperator
case class Exists(expr: SelectStatement) extends UnaryOperator

case class In(elem: Expression, set: Seq[Expression]) extends Expression
case class Case(cond: Expression, thenp: Expression, elsep: Expression) extends Expression
case class Distinct(e: Expression) extends Expression
case class Year(expr: Expression) extends Expression {
  override val isAggregateOpExpr = false
}
case class Substring(expr: Expression, idx1: Expression, idx2: Expression) extends Expression {
  override val isAggregateOpExpr = false
}
case class Upper(expr: Expression) extends Expression {
  override val isAggregateOpExpr = false
}

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null) extends Expression {
  override def toString = qualifier match {
    case Some(q) => q + "." + name
    case None    => name
  }
  override val isAggregateOpExpr = false
}

trait Aggregation extends Expression
case class CountAll() extends Aggregation
case class CountExpr(expr: Expression) extends Aggregation
case class Sum(expr: Expression) extends Aggregation
case class Avg(expr: Expression) extends Aggregation
case class Min(expr: Expression) extends Aggregation
case class Max(expr: Expression) extends Aggregation

trait LiteralExpression extends Expression
case class IntLiteral(v: Int) extends LiteralExpression {
  override def toString = v.toString
}
case class DoubleLiteral(v: Double) extends LiteralExpression {
  override def toString = v.toString
}
case class FloatLiteral(v: Float) extends LiteralExpression {
  override def toString = v.toString
}
case class StringLiteral(v: String) extends LiteralExpression {
  override def toString = "'" + v.toString + "'"
}
case class CharLiteral(v: Char) extends LiteralExpression {
  override def toString = v.toString
}
case object NullLiteral extends LiteralExpression
case class DateLiteral(v: String) extends LiteralExpression {
  override def toString = "DATE '" + v.toString + "'"
}

trait Relation extends Node {
  def extractRelations: Seq[Relation] = this match {
    case Join(left, right, _, _) => left.extractRelations ++ right.extractRelations
    case tbl: SQLTable           => Seq(tbl)
    case sq: Subquery => sq.subquery match {
      case stmt: SelectStatement                  => stmt.extractRelations()
      case UnionIntersectSequence(top, bottom, _) => top.extractRelations ++ bottom.extractRelations
    }
    case vw: View => Seq(vw)
  }

  def extractSubqueries: Seq[Subquery] = this match {
    case Join(left, right, _, _) => left.extractSubqueries ++ right.extractSubqueries
    case tbl: SQLTable           => Seq()
    case sq: Subquery => sq.subquery match {
      case stmt: SelectStatement                  => Seq(sq) ++ sq.subquery.extractSubqueries
      case UnionIntersectSequence(top, bottom, _) => top.extractSubqueries ++ bottom.extractSubqueries
    }
    case vw: View => Seq()
  }
}
case class SQLTable(name: String, alias: Option[String]) extends Relation
// The difference between a subquery and a view is that a subquery is always inlined in a tree,
// while a view is its own operator that buffers data from the parent (thus being a pipeline breaker).
case class Subquery(subquery: TopLevelStatement, alias: String) extends Relation
case class View(subquery: TopLevelStatement, alias: String) extends Relation

sealed abstract trait JoinType
case object InnerJoin extends JoinType
case object LeftSemiJoin extends JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object FullOuterJoin extends JoinType
case object AntiJoin extends JoinType

case class Join(left: Relation, right: Relation, tpe: JoinType, clause: Expression) extends Relation

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class GroupBy(keys: Seq[Expression]) extends Node {
  def contains(e: Expression) = keys.contains(e)
}
case class Having(having: Expression) extends Node
case class OrderBy(keys: Seq[(Expression, OrderType)]) extends Node
case class Limit(rows: Long) extends Node
