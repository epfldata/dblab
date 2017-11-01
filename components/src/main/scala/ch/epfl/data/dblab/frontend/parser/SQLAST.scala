package ch.epfl.data
package dblab
package frontend
package parser

import sc.pardis.types._

import scala.reflect.runtime.{ universe => ru }
//import ru._

/**
 * AST for SQL select statement.
 * Based on: https://github.com/stephentu/scala-sql-parser
 *
 * @author Yannis Klonatos
 */

object SQLAST {

  trait SQLNode
  /*
  trait StreamNode

  trait TopLevelStream extends StreamNode{
    def extractStream(): Seq[] = this match {
      case
    }
  }
*/
  trait TopLevelStatement extends SQLNode with Expression {
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
  // TODO remove the last parameter
  case class UnionIntersectSequence(top: TopLevelStatement, bottom: TopLevelStatement, connectionType: QueryRelationType) extends TopLevelStatement
  sealed trait QueryRelationType
  case object UNIONALL extends QueryRelationType
  case object UNION extends QueryRelationType
  case object INTERSECT extends QueryRelationType
  case object SEQUENCE extends QueryRelationType
  case object EXCEPT extends QueryRelationType
  case class IncludeStatement(include: String, streams: Seq[CreateStatement], body: TopLevelStatement) extends TopLevelStatement
  trait CreateStatement
  case class CreateStream(tag: String, name: String, cols: Seq[(String, Tpe)], rest: String) extends CreateStatement {
    def isTable: Boolean = tag == "TABLE"
    def isStream: Boolean = !isTable
  }
  case class CreateFunction(name: String, inputs: Seq[(String, Tpe)], output: Tpe, external: String) extends CreateStatement
  trait Projections extends SQLNode {
    def size(): Int
    def get(n: Int): (Expression, Option[String])
    def extractAliases(): Seq[(Expression, String, Int)]
    def extractExpretions(): Seq[(Expression, Int)]
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
    override def extractExpretions(): Seq[(Expression, Int)] = lst.zipWithIndex.map(al => (al._1._1, al._2))
    //TODO p._1._2 ?? AVG(A)
  }
  @deprecated("", "")
  case class AllColumns() extends Projections {
    def size(): Int = ???
    def get(n: Int) = ???
    def extractAliases(): Seq[(Expression, String, Int)] = Seq()
    def extractExpretions(): Seq[(Expression, Int)] = Seq()
    def findProjection(e: Expression, alias: String) = ???
  }

  case class StarExpression(relation: Option[String]) extends Expression
  // Expressions
  trait Expression extends SQLNode {
    val isAggregateOpExpr = true
    @deprecated("Use the tpe field instead", "")
    def tp = tpe match {
      case IntType    => ru.typeTag[Int]
      case CharType   => ru.typeTag[Char]
      case DoubleType => ru.typeTag[Double]
      case FloatType  => ru.typeTag[Float]
    }
    var tpe: Tpe = _
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
  case class AllExp(e: SelectStatement) extends Expression
  case class SomeExp(e: SelectStatement) extends Expression
  case class ExtractExp(first: String, second: String) extends Expression
  case class InList(e: Expression, list: List[LiteralExpression]) extends Expression

  case class FunctionExp(name: String, inputs: List[Expression]) extends Expression

  object ExternalFunctionExp {
    def unapply(exp: Expression): Option[(String, List[Expression])] = exp match {
      case FunctionExp(name, inputs) =>
        Some((name, inputs))
      case Substring(e1, e2, e3) =>
        val name = "substring"
        val inputs = List(e1, e2, e3)
        Some((name, inputs))
      case _ => None
    }
  }

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

  case class BoolLiteral(v: Boolean) extends LiteralExpression {
    override def toString = v.toString
  }

  case object NullLiteral extends LiteralExpression
  case class DateLiteral(v: String) extends LiteralExpression {
    override def toString = "DATE '" + v.toString + "'"
  }
  case class IntervalLiteral(s: String, id: String, num: Option[Int]) extends LiteralExpression {
    override def toString: String = id + ": " + s
  }

  object ExpressionShape {
    def unapply(e: Expression): Option[(Seq[Expression] => Expression, Seq[Expression])] =
      e match {
        case tl: TopLevelStatement =>
          throw new Exception(s"Make sure that you pattern match TopLevelStatement before ExpressionShape: $tl")
        case Or(e1, e2)                => Some((l) => Or(l(0), l(1)), Seq(e1, e2))
        case And(e1, e2)               => Some((l) => And(l(0), l(1)), Seq(e1, e2))
        case Equals(e1, e2)            => Some((l) => Equals(l(0), l(1)), Seq(e1, e2))
        case NotEquals(e1, e2)         => Some((l) => NotEquals(l(0), l(1)), Seq(e1, e2))
        case LessOrEqual(e1, e2)       => Some((l) => LessOrEqual(l(0), l(1)), Seq(e1, e2))
        case LessThan(e1, e2)          => Some((l) => LessThan(l(0), l(1)), Seq(e1, e2))
        case GreaterOrEqual(e1, e2)    => Some((l) => GreaterOrEqual(l(0), l(1)), Seq(e1, e2))
        case GreaterThan(e1, e2)       => Some((l) => GreaterThan(l(0), l(1)), Seq(e1, e2))
        case Like(e1, e2)              => Some((l) => Like(l(0), l(1)), Seq(e1, e2))
        case Add(e1, e2)               => Some((l) => Add(l(0), l(1)), Seq(e1, e2))
        case Subtract(e1, e2)          => Some((l) => Subtract(l(0), l(1)), Seq(e1, e2))
        case Multiply(e1, e2)          => Some((l) => Multiply(l(0), l(1)), Seq(e1, e2))
        case Divide(e1, e2)            => Some((l) => Divide(l(0), l(1)), Seq(e1, e2))
        case StringConcat(e1, e2)      => Some((l) => StringConcat(l(0), l(1)), Seq(e1, e2))
        case CountExpr(e1)             => Some((l) => CountExpr(l(0)), Seq(e1))
        case Sum(e1)                   => Some((l) => Sum(l(0)), Seq(e1))
        case Avg(e1)                   => Some((l) => Avg(l(0)), Seq(e1))
        case Min(e1)                   => Some((l) => Min(l(0)), Seq(e1))
        case Max(e1)                   => Some((l) => Max(l(0)), Seq(e1))
        case Year(e1)                  => Some((l) => Year(l(0)), Seq(e1))
        case Upper(e1)                 => Some((l) => Upper(l(0)), Seq(e1))
        case Distinct(e1)              => Some((l) => Distinct(l(0)), Seq(e1))
        case AllExp(e1)                => Some((l) => AllExp(l(0).asInstanceOf[SelectStatement]), Seq(e1))
        case SomeExp(e1)               => Some((l) => SomeExp(l(0).asInstanceOf[SelectStatement]), Seq(e1))
        case Not(e1)                   => Some((l) => Not(l(0)), Seq(e1))
        case Abs(e1)                   => Some((l) => Abs(l(0)), Seq(e1))
        case UnaryPlus(e1)             => Some((l) => UnaryPlus(l(0)), Seq(e1))
        case UnaryMinus(e1)            => Some((l) => UnaryMinus(l(0)), Seq(e1))
        case Exists(e1)                => Some((l) => Exists(l(0).asInstanceOf[SelectStatement]), Seq(e1))
        case Case(e1, e2, e3)          => Some((l) => Case(l(0), l(1), l(2)), Seq(e1, e2, e3))
        case In(e1, l2)                => Some((l) => In(l(0), l.tail), e1 +: l2)
        case InList(e1, l2)            => Some((l) => InList(l(0), l.tail.asInstanceOf[List[LiteralExpression]]), e1 +: l2)
        case FunctionExp(name, inputs) => Some((l) => FunctionExp(name, l.toList), inputs)
        case Substring(e1, e2, e3)     => Some((l) => Substring(l(0), l(1), l(2)), List(e1, e2, e3))
        case (_: LiteralExpression) |
          CountAll() |
          ExtractExp(_, _) => Some((l) => e, Nil)
        case _ => None
      }
  }

  trait Relation extends SQLNode {
    def extractRelations: Seq[Relation] = this match {
      case Join(left, right, _, _) => left.extractRelations ++ right.extractRelations
      case tbl: SQLTable           => Seq(tbl)
      case sq: Subquery => sq.subquery match {
        case stmt: SelectStatement                  => stmt.extractRelations()
        case UnionIntersectSequence(top, bottom, _) => top.extractRelations ++ bottom.extractRelations
      }
      case vw: View => Seq(vw)
    }

    def extractTables: Seq[SQLTable] = this match {
      case st @ SQLTable(_, _)     => Seq(st)
      case Subquery(_, _)          => Seq()
      case Join(left, right, _, _) => left.extractTables ++ right.extractTables
      case _                       => Seq()
    }

    def extractImmediateSubqueries: Seq[Subquery] = this match {
      case Join(left, right, _, _) => left.extractImmediateSubqueries ++ right.extractImmediateSubqueries
      case tbl: SQLTable           => Seq()
      case sq: Subquery            => Seq(sq)
      case vw: View                => Seq()
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
  case object NaturalJoin extends JoinType
  case object AntiJoin extends JoinType

  case class Join(left: Relation, right: Relation, tpe: JoinType, clause: Expression) extends Relation

  sealed abstract trait OrderType
  case object ASC extends OrderType
  case object DESC extends OrderType

  case class GroupBy(keys: Seq[Expression]) extends SQLNode {
    def contains(e: Expression) = keys.contains(e)
  }
  case class Having(having: Expression) extends SQLNode
  case class OrderBy(keys: Seq[(Expression, OrderType)]) extends SQLNode
  case class Limit(rows: Long) extends SQLNode
}