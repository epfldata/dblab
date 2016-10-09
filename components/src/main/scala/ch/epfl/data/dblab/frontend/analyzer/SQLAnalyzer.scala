package ch.epfl.data
package dblab
package frontend
package analyzer

import sc.pardis.types._
import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._
import parser._
import sc.pardis.shallow.OptimalString

/**
 *
 * @author Yannis Klonatos
 */
class SQLAnalyzer(schema: Schema) {
  var aliasesList: Seq[(Expression, String, Int)] = Seq()
  var views: Seq[View] = Seq()

  // TODO: Maybe this should be removed if there is a better solution for it
  def typeToTypeTag(tp: Tpe) = tp match {
    case c if c == IntType || c == DateType => typeTag[Int]
    case c if c == DoubleType               => typeTag[Double]
    case c if c == CharType                 => typeTag[Char]
    case c: VarCharType                     => typeTag[VarCharType]
  }

  def setResultType(e: Expression, left: Expression, right: Expression) {
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    (left.tp, right.tp) match {
      case (x, y) if x == y        => e.setTp(x)
      case (IntType, DoubleType)   => e.setTp(DoubleType)
      case (DoubleType, IntType)   => e.setTp(DoubleType)
      case (DoubleType, FloatType) => e.setTp(DoubleType)
      // The following may actually happen (see referenced TPCDS queries and expression for more details)
      case (IntType, null)         => e.setTp(IntType) // TPCDS Q21, expr CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE null END)
      case (DoubleType, null)      => e.setTp(DoubleType) // TPCDS Q43, expr SUM(CASE WHEN (d_day_name='Saturday') THEN ss_sales_price ELSE null END) sat_sales
      case (null, IntType)         => e.setTp(IntType) // TPCDS Q59
      case (null, DoubleType)      => e.setTp(DoubleType)
    }
  }

  def checkAndInferExpr(expr: Expression): Unit = expr match {
    // Literals
    case lt @ (DateLiteral(_) | IntLiteral(_)) =>
      lt.setTp(typeTag[Int])
    case fl @ FloatLiteral(_) =>
      fl.setTp(typeTag[Float])
    case dl @ DoubleLiteral(_) =>
      dl.setTp(typeTag[Double])
    case sl @ StringLiteral(_) =>
      sl.setTp(typeTag[OptimalString])
    case cl @ CharLiteral(_) =>
      cl.setTp(typeTag[Char])
    case nl @ NullLiteral =>
      nl.setTp(null) // ??? is this correct
    case fi @ FieldIdent(_, name, _) =>
      schema.findAttribute(name) match {
        case Some(a) =>
          fi.setTp(typeToTypeTag(a.dataType))
        case None =>
          aliasesList.find(al => al._2 == name) match {
            case Some(al) => al._1.tp match {
              case null =>
                val projs = views.map(v => v.subquery.findProjection(al._1, al._2)).flatten
                projs.size match {
                  case 0 => throw new Exception("SQLSemanticCheckerAndTypeInference BUG: No projection in any view exists for " + al._1 + "/" + al._2)
                  case 1 => fi.setTp(projs(0).tp)
                  case _ =>
                    val tps = projs.map(_.tp).distinct
                    if (tps.size != 1)
                      throw new Exception("SQLSemanticCheckerAndTypeInference BUG: Too many (" + projs.size + ") projections in views exist of different types " + tps + " for " + al._1 + "/" + al._2)
                    fi.setTp(tps(0))
                }
              case _ => fi.setTp(al._1.tp)
            }
            case None => throw new Exception("Attribute " + name + " referenced in SQL query does not exist in any relation or in any alias.")
          }
      }
    // Arithmetic Operators
    case add @ Add(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(add, left, right)
    case sub @ Subtract(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(sub, left, right)
    case mut @ Multiply(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(mut, left, right)
    case div @ Divide(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(div, left, right)
    case uminus @ UnaryMinus(expr) =>
      checkAndInferExpr(expr)
      uminus.setTp(expr.tp)
    case sum @ Sum(expr) =>
      checkAndInferExpr(expr)
      sum.setTp(typeTag(expr.tp))
    case avg @ Avg(expr) =>
      checkAndInferExpr(expr)
      avg.setTp(typeTag(expr.tp))
    case countAll @ CountAll() =>
      countAll.setTp(typeTag[Int])
    case countExpr @ CountExpr(expr) =>
      checkAndInferExpr(expr)
      countExpr.setTp(typeTag[Int])
    case min @ Min(expr) =>
      checkAndInferExpr(expr)
      min.setTp(expr.tp)
    case max @ Max(expr) =>
      checkAndInferExpr(expr)
      max.setTp(expr.tp)
    case abs @ Abs(expr) =>
      checkAndInferExpr(expr)
      abs.setTp(expr.tp)
    // Logical Operators
    case and @ And(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case and @ Or(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case eq @ Equals(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      eq.setTp(typeTag[Boolean])
    case neq @ NotEquals(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      neq.setTp(typeTag[Boolean])
    case lt @ LessThan(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      lt.setTp(typeTag[Boolean])
    case loe @ LessOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      loe.setTp(typeTag[Boolean])
    case gt @ GreaterThan(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      gt.setTp(typeTag[Boolean])
    case goe @ GreaterOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      goe.setTp(typeTag[Boolean])
    case strcon @ StringConcat(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      strcon.setTp(left.tp)
    case upper @ Upper(expr) =>
      checkAndInferExpr(expr)
      upper.setTp(expr.tp)
    case not @ Not(expr) =>
      checkAndInferExpr(expr)
      not.setTp(expr.tp)
    // SQL Statements
    case yr @ Year(date) =>
      checkAndInferExpr(date)
      yr.setTp(typeTag[Int])
    case lk @ Like(field, values) =>
      checkAndInferExpr(field)
      checkAndInferExpr(values)
      lk.setTp(field.tp)
    case ex @ Exists(nestedQuery) =>
      checkAndInfer(nestedQuery)
      ex.setTp(nestedQuery.tp)
    case cs @ Case(cond, thenp, elsep) =>
      checkAndInferExpr(cond)
      checkAndInferExpr(thenp)
      checkAndInferExpr(elsep)
      setResultType(cs, thenp, elsep)
    case in @ In(fld, listExpr) =>
      checkAndInferExpr(fld)
      listExpr.foreach(e => checkAndInferExpr(e))
      in.setTp(typeTag[Boolean])
    case substr @ Substring(fld, idx1, idx2) =>
      checkAndInferExpr(fld)
      checkAndInferExpr(idx1)
      checkAndInferExpr(idx2)
      substr.setTp(fld.tp)
    case distinct @ Distinct(e) =>
      checkAndInferExpr(e)
      distinct.setTp(e.tp)
    case e: SelectStatement => // Nested subquery
      checkAndInfer(e)
      e.projections match {
        case ep: ExpressionProjections => ep.size match {
          case 0 => throw new Exception("SQLSemanticCheckerAndTypeInference BUG: Subquery has no projection, thus it is impossible to infer its type!")
          case 1 => e.setTp(e.projections.get(0)._1.tp)
          case _ =>
            val tps = ep.lst.map(_._1.tp)
            if (tps.size != 1)
              throw new Exception("SQLSemanticCheckerAndTypeInference BUG: Too many (" + e.projections.size + ") projections in subquery exist of different types " + tps)
            e.setTp(tps(0))
        }
        case ac: AllColumns => // Do nothing and pray noone will notice :)
      }
  }

  def checkAndInferJoinTree(root: Relation): Unit = root match {
    case Join(leftParent, Subquery(subquery, _), _, clause) =>
      checkAndInferJoinTree(leftParent)
      checkAndInfer(subquery)
      checkAndInferExpr(clause)
    case Join(Subquery(subquery, _), rightParent, _, clause) =>
      checkAndInferJoinTree(rightParent)
      checkAndInfer(subquery)
      checkAndInferExpr(clause)
    case Subquery(parent, _) => checkAndInfer(parent)
    case Join((leftParent: Join), SQLTable(_, _), _, clause) =>
      checkAndInferExpr(clause)
      checkAndInferJoinTree(leftParent)
    case Join((leftParent: Join), View(_, _), _, clause) =>
      checkAndInferExpr(clause)
      checkAndInferJoinTree(leftParent)
    // Nothing to infer for the following
    case Join(SQLTable(_, _), SQLTable(_, _), _, clause) =>
      checkAndInferExpr(clause)
    case SQLTable(_, _) =>
    // For unknown cases, throw an exception to keep track of them and add them above if necessary
    case dflt           => throw new Exception("Unknown class type " + dflt + " encountered in SQLSemanticCheckerAndTypeInference component!")
  }

  def checkAndInfer(node: Node) {
    node match {
      case UnionIntersectSequence(top, bottom, _) =>
        checkAndInfer(top)
        checkAndInfer(bottom)
      case stmt: SelectStatement => checkAndInfer(stmt)
    }
  }

  def checkAndInfer(sqlTree: SelectStatement) {
    // First, save some info in global vars for later use
    aliasesList = sqlTree.aliases ++ aliasesList
    views = views ++ sqlTree.withs
    // Then start inferring
    sqlTree.withs.foreach(w => checkAndInfer(w.subquery))
    sqlTree.joinTree match {
      case Some(tr) => checkAndInferJoinTree(tr)
      case None     =>
    }
    sqlTree.where match {
      case Some(expr) => checkAndInferExpr(expr)
      case None       =>
    }
    sqlTree.projections match {
      case ExpressionProjections(proj) => proj.foreach(p => checkAndInferExpr(p._1))
      case AllColumns()                =>
    }
    sqlTree.groupBy match {
      case Some(GroupBy(listExpr)) => listExpr.foreach(expr => checkAndInferExpr(expr))
      case None                    =>
    }
    sqlTree.orderBy match {
      case Some(OrderBy(listExpr)) => listExpr.foreach(expr => checkAndInferExpr(expr._1))
      case None                    =>
    }
    sqlTree.having match {
      case Some(clause) => checkAndInferExpr(clause.having)
      case None         =>
    }
  }
}
