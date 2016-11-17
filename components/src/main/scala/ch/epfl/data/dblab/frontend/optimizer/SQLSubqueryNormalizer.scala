package ch.epfl.data
package dblab
package frontend
package optimizer

import schema.Schema
import parser._

/**
 * Converts SQL queries with subqueries into normalized queries without subqueries.
 *
 * @author Yannis Klonatos
 */
class SQLSubqueryNormalizer(schema: Schema) extends SQLNormalizer {

  private var temporaryViewsCounter = 0
  def getTemporaryViewName = {
    val name = "__temporary_view_" + temporaryViewsCounter.toString
    temporaryViewsCounter += 1
    name
  }

  override def normalizeStmt(stmt: SelectStatement): SelectStatement = {
    val subqueriesToBeRewritten = findSubqueriesToBeRewritten(stmt)
    //System.out.println("Subqueries to be rewritten: " + subqueriesToBeRewritten.mkString("\n"))
    transformQuery(stmt, subqueriesToBeRewritten)
  }

  // Analysis phase
  private def findSubqueriesToBeRewritten(stmt: SelectStatement): Seq[TopLevelStatement] = {
    stmt.where.map(extractSubqueriesFromExpression(_)).getOrElse(Seq())
  }

  private def extractSubqueriesFromExpression(expr: Expression): Seq[TopLevelStatement] = expr match {
    case unrOp: UnaryOperator                            => extractSubqueriesFromExpression(unrOp.expr)
    case binOp: BinaryOperator                           => extractSubqueriesFromExpression(binOp.left) ++ extractSubqueriesFromExpression(binOp.right)

    case fld: FieldIdent                                 => Seq()
    case literal if expr.isInstanceOf[LiteralExpression] => Seq()
    case subq: SelectStatement                           => if (subq.where.isDefined && queryMustBeRewritten(subq.where.get, subq.extractRelations)) Seq(subq) else Seq()
    case In(expr, listExpr)                              => extractSubqueriesFromExpression(expr) ++ listExpr.map(extractSubqueriesFromExpression(_)).flatten
    case Case(cond, thenp, elsep)                        => extractSubqueriesFromExpression(cond) ++ extractSubqueriesFromExpression(thenp) ++ extractSubqueriesFromExpression(elsep)
  }

  private def queryMustBeRewritten(expr: Expression, relations: Seq[Relation]): Boolean = expr match {
    case And(left, right)            => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)
    case GreaterThan(left, right)    => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)
    case GreaterOrEqual(left, right) => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)
    case LessOrEqual(left, right)    => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)
    case Equals(left, right)         => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)
    case Add(left, right)            => queryMustBeRewritten(left, relations) || queryMustBeRewritten(right, relations)

    case fld: FieldIdent =>
      containsField(fld, relations)
    case lit: LiteralExpression =>
      false
  }

  private def containsField(field: FieldIdent, relations: Seq[Relation]): Boolean = {
    val (fldName, fldIdentifier) = (field.name, field.qualifier)
    relations.find(rel => rel match {
      case SQLTable(tableName, alias) => fldIdentifier match {
        case Some(id) => fldIdentifier == alias
        case None     => fldName == tableName
      }
    }).isDefined
  }

  // Transformation phase
  private def transformQuery(stmt: SelectStatement, subqueriesToBeRewritten: Seq[TopLevelStatement]): SelectStatement = {
    val (newWhere, newJoinTree, additionalViews) = stmt.where match {
      case Some(w) =>
        val (newExpr, newViews, remCond) = transformExpr(w, subqueriesToBeRewritten)
        val finalExpr = (newExpr, remCond) match {
          case (Some(e), Seq(elems)) => remCond.foldLeft(e)((elem, acc) => And(acc, elem))
          case (Some(e), Seq())      => e
        }
        val newJoinTree = stmt.joinTree match {
          case Some(tr) => Some(newViews.foldLeft(tr)((elem, acc) => Join(elem, acc, InnerJoin, Equals(IntLiteral(1), IntLiteral(1)))))
          case None     => None
        }
        (Some(finalExpr), newJoinTree, newViews)
      case None => (stmt.where, stmt.joinTree, Seq())
    } //.getOrElse((stmt.where, Seq()))
    SelectStatement(stmt.withs ++ additionalViews, stmt.projections, newJoinTree, newWhere, stmt.groupBy, stmt.having, stmt.orderBy, stmt.limit, stmt.aliases)
  }

  private def transormSubquery(subq: SelectStatement, subqueriesToBeRewritten: Seq[TopLevelStatement]): (View, Seq[Expression]) = {
    //System.out.println("Searching for subq " + subq + " in " + subqueriesToBeRewritten + " = " + subqueriesToBeRewritten.find(sqnr => sqnr == subq))
    val (groupBy, remCond) = subq.where match {
      case Some(w) =>
        val newExpr = extractFromRewrittenClause(w, subq.extractRelations)
        if (newExpr._1.size != 0)
          (Some(newExpr._1), newExpr._2)
        else (None, newExpr._2)
      case None => (subq.groupBy.map(g => Some(g.keys)), Seq())
    }

    val newProjections = subq.projections match {
      case AllColumns() => AllColumns()
      case ep: ExpressionProjections => ExpressionProjections(ep.lst ++ {
        groupBy match {
          case Some(gb) => gb.asInstanceOf[Seq[Expression]].map(gb => (gb, Some(gb match {
            case FieldIdent(_, name, _) => name
          })))
          case None => Seq()
        }
      })
    }

    // THIS needs more work -- it shouldn't be None in where or join clause and i doubt the newStmt clause is generally correct
    val newGroupBy = groupBy match {
      case Some(gb) => Some(GroupBy(gb.asInstanceOf[Seq[Expression]]))
      case None     => None
    }
    val newView = View(SelectStatement(subq.withs, newProjections, subq.joinTree, None, newGroupBy, subq.having, subq.orderBy, subq.limit, subq.aliases), getTemporaryViewName)
    (newView, remCond)
  }

  private def regenerateExpr(expr: Expression, op1: Expression, op2: Expression = null) = expr match {
    case and: And            => And(op1, op2)
    case or: Or              => Or(op1, op2)
    case lt: LessThan        => LessThan(op1, op2)
    case lor: LessOrEqual    => LessOrEqual(op1, op2)
    case eq: Equals          => Equals(op1, op2)
    case gt: GreaterThan     => GreaterThan(op1, op2)
    case goe: GreaterOrEqual => GreaterOrEqual(op1, op2)
    case lk: Like            => Like(op1, op2)
    case add: Add            => Add(op1, op2)
    case sub: Subtract       => Subtract(op1, op2)
    case div: Divide         => Divide(op1, op2)
    case cs: Case            => Case(cs.cond, op1, op2)
    case mul: Multiply       => Multiply(op1, op2)
  }

  private def regenerateBinaryOperator(expr: Expression, left: Expression, right: Expression, subqueriesToBeRewritten: Seq[TopLevelStatement]) = {
    val (newLeft, newViewsLeft, remCondLeft) = transformExpr(left, subqueriesToBeRewritten)
    val (newRight, newViewsRight, remCondRight) = transformExpr(right, subqueriesToBeRewritten)
    (newLeft, newRight) match {
      case (Some(l), Some(r)) =>
        val newExpr = Some(regenerateExpr(expr, l, r))
        (newExpr, newViewsLeft ++ newViewsRight, remCondLeft ++ remCondRight)
      case (Some(l), None) => (newLeft, newViewsLeft ++ newViewsRight, remCondLeft ++ remCondRight)
      case (None, Some(r)) => (newRight, newViewsLeft ++ newViewsRight, remCondLeft ++ remCondRight)
      case (None, None)    => (None, newViewsRight ++ newViewsRight, remCondLeft ++ remCondRight)
    }
  }

  private def regenerateUnaryOperator(expr: Expression, left: Expression, subqueriesToBeRewritten: Seq[TopLevelStatement]) = {
    val (newExpr, newViews, remCond) = transformExpr(left, subqueriesToBeRewritten)
    newExpr match {
      case Some(l) => (Some(regenerateExpr(expr, l)), newViews, remCond)
      case None    => (None, newViews, remCond)
    }
  }

  private def isNotSubquery(expr: Expression) = !expr.isInstanceOf[SelectStatement]

  private def transformExpr(expr: Expression, subqueriesToBeRewritten: Seq[TopLevelStatement]): (Option[Expression], Seq[View], Seq[Expression]) = expr match {
    case GreaterThan(left, (sq: SelectStatement)) =>
      if (sq.projections.size > 1)
        throw new Exception("Subquery used in GreaterThan expression, but subquery returns more than one attribute as result!")

      if (subqueriesToBeRewritten.contains(sq)) {
        val newSubq = transormSubquery(sq, subqueriesToBeRewritten)
        val proj = FieldIdent(None, newSubq._1.subquery.asInstanceOf[SelectStatement].projections.get(0)._2.get)
        val newExpr = Some(GreaterThan(left, proj))
        (newExpr, Seq(newSubq._1), newSubq._2)
      } else (Some(expr), Seq(), Seq())

    case fld: FieldIdent => (Some(fld), Seq(), Seq())
    case literal if expr.isInstanceOf[LiteralExpression] => (Some(literal), Seq(), Seq())
    case in: In => (Some(in), Seq(), Seq())
    case unrOp: UnaryOperator => regenerateUnaryOperator(expr, unrOp.expr, subqueriesToBeRewritten)
    case binOp: BinaryOperator => regenerateBinaryOperator(expr, binOp.left, binOp.right, subqueriesToBeRewritten)
    case cs: Case => (Some(cs), Seq(), Seq())
  }

  // Obviously this needs to be generalized -- also it needs to progressively build a bigger expression
  private def extractFromRewrittenClause(expr: Expression, relations: Seq[Relation]): (Seq[Expression], Seq[Expression]) = expr match {
    case Equals(left: FieldIdent, right: FieldIdent) =>
      if (containsField(left, relations)) (Seq(left), Seq(expr))
      else if (containsField(right, relations)) (Seq(right), Seq(expr))
      else (Seq(), Seq())
  }
}
