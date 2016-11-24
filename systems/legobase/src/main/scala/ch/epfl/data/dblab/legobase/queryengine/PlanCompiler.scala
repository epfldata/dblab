package ch.epfl.data
package dblab
package queryengine

import ch.epfl.data.dblab.schema._
import ch.epfl.data.sc.pardis.ir.Constant
import ch.epfl.data.sc.pardis.types.{ PardisType, RecordType }
import ch.epfl.data.sc.pardis.types.PardisTypeImplicits._
import ch.epfl.data.sc.pardis.shallow.Record
import legobase.deep.LegoBaseQueryEngineExp
import ch.epfl.data.sc.pardis.deep.scalalib.ArrayOps

import ch.epfl.data.dblab.storagemanager.{ Loader => ShallowLoader }
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import frontend.parser.OperatorAST._
import frontend.parser.SQLAST._
import config.Config

/**
 * The main module for compiling query plans.
 *
 * @author Daniel Espino
 */
object PlanCompiler { this: LegoBaseQueryEngineExp =>
  type Rep[T] = LegoBaseQueryEngineExp#Rep[T]
  val context = new LegoBaseQueryEngineExp {}
  import context._

  val activeViews = new scala.collection.mutable.HashMap[String, ViewOp[_]]()

  /**
   * Interprets the given query for the given schema
   *
   * @param query the input operator tree
   * @param schema the input schema
   */
  def executeQuery(operatorTree: QueryPlanTree, schema: Schema): Unit = {
    def block = {
      // Execute main query plan tree
      val qp = queryToOperator(operatorTree, schema).asInstanceOf[Rep[PrintOp[Record]]]
      // Execute tree
      // qp.open
      // qp.next
      qp.run()
    }

    import legobase.compiler._
    val settings = new Settings(List("-scala", "+no-sing-hm"))
    val validatedSettings = settings.validate()
    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.experimentation.tpch.TPCHRunner") {
      override def outputFile = "out"
    }
    compiler.compile(block)
  }

  def queryToOperator(operatorTree: QueryPlanTree, schema: Schema): Rep[Operator[Record]] = {
    // Calculate the views if any
    // operatorTree.views.foreach(v => {
    //   val vo = new ViewOp(convertOperator(v.parent))
    //   vo.open
    //   activeViews += v.name -> vo
    // })

    convertOperator(operatorTree.rootNode)
  }

  // TODO -- Generalize! 
  def parseJoinClause(e: Expression): (Expression, Expression) = e match {
    case Equals(left, right) => (left, right)
    case And(left, equals)   => parseJoinClause(left)
    case GreaterThan(_, _)   => throw new Exception("LegoBase currently does not support Theta-Joins. This is either a problem with the query (e.g. GreaterThan on ON join clause) or a bug of the optimizer. ")
  }

  def createJoinOperator(node: OperatorNode, leftParent: OperatorNode, rightParent: OperatorNode, joinCond: Expression, joinType: JoinType, leftAlias: String, rightAlias: String): Rep[Operator[Record]] = {
    val leftOp = convertOperator(leftParent)
    val rightOp = convertOperator(rightParent)
    val (leftCond, rightCond) = parseJoinClause(joinCond)

    val joinFun = __lambda[Record, Record, Boolean] { (x, y) =>
      val comp = new ExpressionCompiler {
        override val context = PlanCompiler.context
        override def getField[T: PardisType](qualifier: Option[String], fieldName: String): Option[Rep[T]] = {
          if (qualifier.isDefined && qualifier.get == leftAlias) {
            leftParent.resultType.getField[T](x, None, fieldName)
          } else if (qualifier.isDefined && qualifier.get == rightAlias) {
            rightParent.resultType.getField[T](y, None, fieldName)
          } else {
            leftParent.resultType.getField[T](x, qualifier, fieldName).orElse {
              rightParent.resultType.getField[T](y, qualifier, fieldName)
            }
          }
        }
      }
      import comp._
      joinCond.asRep[Boolean]
    }

    val leftFun = __lambda[Record, Any] { x =>
      val comp = leftParent.resultType.expressionCompiler(x)
      import comp._
      leftCond.toRep
    }

    val rightFun = __lambda[Record, Any] { x =>
      val comp = rightParent.resultType.expressionCompiler(x)
      import comp._
      rightCond.toRep
    }

    val leftTp = ExpressionCompiler.ExpressionOps(leftCond).pardisType.asInstanceOf[PardisType[Any]]

    joinType match {
      case LeftSemiJoin =>
        __newLeftHashSemiJoinOp[Record, Record, Any](leftOp, rightOp)(joinFun)(leftFun)(rightFun)(
          leftParent.resultType.pardisType, rightParent.resultType.pardisType, leftTp)
      // case LeftOuterJoin =>
      //   //TODO Generalize!
      //   val tp = rightParent match {
      //     case ScanOpNode(_, _, _)                    => classTag[DynamicDataRow]
      //     case SelectOpNode(parent: ScanOpNode, _, _) => classTag[DynamicDataRow]
      //   }
      //   val rightTp = Manifest.classType(tp.runtimeClass).asInstanceOf[Manifest[Record]]
      //   new LeftOuterJoinOp(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
      //     x => parseExpression(leftCond, x)(leftCond.tp)
      //   )(x => parseExpression(rightCond, x)(rightCond.tp))(rightTp)
      case AntiJoin =>
        __newHashJoinAnti[Record, Record, Any](leftOp, rightOp)(joinFun)(leftFun)(rightFun)(
          implicitly[Manifest[Record]], leftParent.resultType.pardisType, rightParent.resultType.pardisType, leftTp)
      case InnerJoin =>
        __newHashJoinOp[Record, Record, Any](leftOp, rightOp, unit(leftAlias), unit(rightAlias))(joinFun)(leftFun)(rightFun)(
          implicitly[Overloaded1], leftParent.resultType.pardisType, rightParent.resultType.pardisType, leftTp)
    }
  }

  def createAggOpOperator(node: OperatorNode, parentOp: OperatorNode, aggs: Seq[Expression], gb: Seq[(Expression, String)], aggNames: Seq[String]): Rep[Operator[Record]] = {
    System.out.println(s"AGGOP INFO: $aggs")
    System.out.println(gb)
    System.out.println(aggNames)

    val aggFuncs: Seq[Rep[(Record, Double) => Double]] = aggs.map { aggExpression =>
      __lambda[Record, Double, Double] { (t, currAgg) =>
        val expressionCompiler = parentOp.resultType.expressionCompiler(t)
        import expressionCompiler._
        val result = aggExpression match {
          case Sum(e) =>
            // TODO: is it necessary to promote the result of e.asRep?
            addition[Double](currAgg, e.asRep[Double])
          case Min(e) =>
            val newMin = e.asRep[Double]
            __ifThenElse((currAgg __== unit(0)) || newMin < currAgg, newMin, currAgg) // TODO -- Assumes that 0 cannot occur naturally in the data as a min value. FIXME
          case CountAll()       => currAgg + unit[Int](1)
          // case CountExpr(expr) => {
          //   // From http://www.techonthenet.com/sql/count.php: "Not everyone realizes this, but the SQL COUNT function will only include the
          //   // records in the count where the value of expression in COUNT(expression) is NOT NULL".
          //   // Here we use pardis default value to test for the above condition
          //   import sc.pardis.shallow.utils.DefaultValue
          //   if (parseExpression(expr, t)(expr.tp) != DefaultValue(expr.tp.tpe.dealias.toString)) currAgg + 1
          //   else currAgg
          // }
          case IntLiteral(v)    => unit(v.toDouble)
          case DoubleLiteral(v) => unit(v)
          case CharLiteral(v)   => unit(v.toDouble)
        }
        result
      }
    }

    val dataRowName = aggRecordNameForFields(gb.map { _._2 })
    val aggTpe = dataRowTypeForName(dataRowName)
    val grp: Rep[(Record) => DynamicDataRow] = __lambda[Record, DynamicDataRow] { t =>
      DynamicDataRow(unit(dataRowName))(gb.map {
        case (exp, name) =>
          val expressionCompiler = parentOp.resultType.expressionCompiler(t)
          import expressionCompiler._
          val value = exp.toRep
          Tuple2(unit(name), value)
      }: _*)
    }

    __newAggOp(
      convertOperator(parentOp),
      unit(aggs.length))(grp)(aggFuncs: _*)(parentOp.resultType.pardisType, aggTpe)
  }

  def createSelectOperator(node: OperatorNode, parentOp: OperatorNode, cond: Expression): Rep[SelectOp[Record]] = {
    __newSelectOp(convertOperator(parentOp))(__lambda { t =>
      val expressionCompiler = parentOp.resultType.expressionCompiler(t)
      import expressionCompiler._
      cond.asRep[Boolean]
    })(node.resultType.pardisType)
  }

  def createMapOperator(node: OperatorNode, parentOp: OperatorNode, indices: Seq[(String, String)]): Rep[MapOp[Record]] = {
    val mapFuncs: Seq[Rep[Record => Unit]] = indices.map {
      case (idx1, idx2) =>
        __lambda[Record, Unit] { t =>
          parentOp.resultType.setField(t, idx1, parentOp.resultType.getField[Double](t, None, idx1).get / parentOp.resultType.getField[Double](t, None, idx2).get)
        }
    }

    __newMapOp(convertOperator(parentOp))(mapFuncs: _*)(node.resultType.pardisType)
  }

  def createSortOperator(node: OperatorNode, parentOp: OperatorNode, orderBy: Seq[(Expression, OrderType)]): Rep[SortOp[Record]] = {
    __newSortOp(convertOperator(parentOp))(__lambda[Record, Record, Int] { (kv1, kv2) =>
      val expressions = orderBy.map { e =>
        // Expression compiler without input tuples
        val exp = new ExpressionCompiler {
          override val context = PlanCompiler.context
          override def getField[T: PardisType](qualifier: Option[String], fieldName: String): Option[Rep[T]] = ???
        }

        val tp = ExpressionCompiler.ExpressionOps(e._1).pardisType
        val k1 = parentOp.resultType.expressionCompiler(kv1).parseExpression(e._1)(tp)
        val k2 = parentOp.resultType.expressionCompiler(kv2).parseExpression(e._1)(tp)

        val res: Rep[Int] = tp match {
          case i if i == typeInt           => exp.subtraction[Int](k1, k2)
          // Multiply with 100 to account for very small differences (e.g. 0.02)
          case d if d == typeDouble        => exp.product[Double](exp.subtraction[Double](k1, k2), unit(100.0)).toInt
          case c if c == typeChar          => exp.subtraction[Char](k1, k2).toInt
          case s if s == typeOptimalString => k1.asInstanceOf[Rep[OptimalString]].diff(k2.asInstanceOf[Rep[OptimalString]])
          //case _                 => (k1.asInstanceOf[Double] - k2.asInstanceOf[Double]) * 100 // TODO -- Type inference bug -- there should be absolutely no need for that and this will soon DIE
        }

        if (e._2 == DESC) -res else res
      }

      // Create a tree a sequence of nested if-then-elses. Whenever the result
      // of an expression is already different from 0 we return the result,
      // otherwise we check the next expression.
      // TODO: a expression shouldn't be computed if a previous one has already
      // returned != 0
      val result = expressions.foldLeft(unit(0)) { (acc, exp) =>
        __ifThenElse[Int](acc __!= unit(0), acc, exp)
      }

      // Break ties arbitrarily
      __ifThenElse[Int](result __!= unit(0), result, infix_hashCode(kv1) - infix_hashCode(kv2))
    }(parentOp.resultType.pardisType, parentOp.resultType.pardisType, typeInt))(node.resultType.pardisType)
  }

  def printRecord(rec: Rep[Record], parent: OperatorNode, order: Seq[Expression] = scala.collection.immutable.Seq()): Rep[Unit] = {
    //System.out.println("Printing record...")
    // TODO: actually evaluate the expressions
    val fieldNames = if (order.isEmpty) {
      parent.resultType.fieldNames
    } else {
      order.map {
        _ match {
          case FieldIdent(qualifier, name, _) =>
            qualifier.getOrElse("") + name
          // TODO
          //case e =>
          //val parsedExpr = parseExpression(e, rec)(e.tp)
          //unit(parsedExpr)
        }
      }
    }

    fieldNames.foreach { name =>
      val tpe = parent.resultType.fields.getOrElse(name, throw new Exception(s"Could not find field $name"))
      val f = parent.resultType.getField(rec, None, name)(tpe.tp).get
      tpe.tp match {
        case IntType           => printf(unit("%d"), f)
        case CharType          => printf(unit("%c"), f)
        case DoubleType        => printf(unit(s"%.${tpe.outputPrecision}f"), f) // TODO -- Precision should not be hardcoded
        case StringType        => printf(unit("%s"), f)
        case VarCharType(_)    => printf(unit("%s"), f)
        case OptimalStringType => printf(unit("%s"), f)
        case DateType          => printf(unit("%s"), f)
        case _ =>
          throw new Exception(s"Do not know how to print member $name of type $tpe.")
      }
      if (name != fieldNames.last) printf(unit("|"))
    }

    printf(unit("\n"))
  }

  def createPrintOperator(node: OperatorNode, parent: OperatorNode, projs: Seq[(Expression, Option[String])], limit: Int): Rep[PrintOp[Record]] = {
    val finalProjs = projs.map(p => p._1 match {
      case c if (!p._2.isDefined && p._1.isAggregateOpExpr) =>
        throw new Exception("LegoBase limitation: Aggregates must always be aliased (e.g. SUM(...) AS TOTAL) -- aggregate " + p._1 + " was not ")
      case _ => p._1 match {
        case agg if p._1.isAggregateOpExpr => FieldIdent(None, p._2.get)
        // TODO -- The following line shouldn't be necessary in a clean solution
        case _: Year                       => FieldIdent(None, p._2.get)
        case _: Substring                  => FieldIdent(None, p._2.get)
        case c if p._2.isDefined           => FieldIdent(None, p._2.get)
        case _                             => p._1 // last chance
      }
    })

    __newPrintOp(convertOperator(parent))(__lambda { kv =>
      printRecord(kv, parent, finalProjs)
    }, unit(limit))(node.resultType.pardisType)
  }

  def aggRecordNameForFields(fields: Seq[String]): String = {
    "AggKey" + fields.foldLeft("")(_ + "_" + _)
  }

  def convertOperator(node: OperatorNode): Rep[Operator[Record]] = {
    node match {
      case ScanOpNode(table, _, _) =>
        __newScanOp(Loader.loadUntypedTable(unit(table)))(dataRowTypeForTable(table))
      case SelectOpNode(parent, cond, _) =>
        createSelectOperator(node, parent, cond)
      case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
        createJoinOperator(node, leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias)
      case AggOpNode(parent, aggs, gb, aggAlias) =>
        createAggOpOperator(node, parent, aggs, gb, aggAlias)
      case MapOpNode(parent, indices) =>
        createMapOperator(node, parent, indices)
      case OrderByNode(parent, orderBy) =>
        createSortOperator(node, parent, orderBy)
      case PrintOpNode(parent, projNames, limit) =>
        createPrintOperator(node, parent, projNames, limit)
      case SubqueryNode(parent) =>
        convertOperator(parent)
      case SubquerySingleResultNode(parent) =>
        val p = convertOperator(parent)
        __newSubquerySingleResult(p)(parent.resultType.pardisType)
      // case UnionAllOpNode(top, bottom) =>
      //   // new UnionAllOperator(convertOperator(top), convertOperator(bottom))
      //   ??? // TODO
      case ProjectOpNode(parent, projNames, origFieldNames) =>
        // TODO
        // Maybe all that we need to do is hide the other fields?
        //new ProjectOperator(convertOperator(parent), projNames, origFieldNames)
        convertOperator(parent)
      // case ViewOpNode(_, _, name) => new ScanOp(activeViews(name).getDataArray())
    }
  }

  implicit class OperatorNodeOps(node: OperatorNode) {
    type Seq[T] = collection.immutable.Seq[T]
    val Seq = collection.immutable.Seq

    def parents: Seq[OperatorNode] = node match {
      case ScanOpNode(table, _, _)               => Seq()
      case SelectOpNode(parent, _, _)            => Seq(parent)
      case JoinOpNode(left, right, _, _, _, _)   => Seq(left, right)
      case AggOpNode(parent, aggs, gb, aggNames) => Seq(parent)
      case MapOpNode(parent, _)                  => Seq(parent)
      case OrderByNode(parent, _)                => Seq(parent)
      case PrintOpNode(parent, _, _)             => Seq(parent)
      case UnionAllOpNode(top, bottom)           => Seq(top, bottom)
      case ProjectOpNode(parent, projNames, _)   => Seq(parent)
      case ViewOpNode(parent, projNames, _)      => Seq(parent)
      case SubqueryNode(parent)                  => Seq(parent)
      case SubquerySingleResultNode(parent)      => Seq(parent)
    }

    def resultType: ResultRep = ResultRep(node)
  }

  abstract class FieldType {
    val tp: PardisType[_]
    val outputPrecision: Int
  }
  case class SimpleField(tp: PardisType[_], outputPrecision: Int = 2) extends FieldType
  case class AggKeyField(tp: PardisType[_], outputPrecision: Int = 2) extends FieldType
  case class AggResultField(index: Int, tp: PardisType[_], outputPrecision: Int = 4) extends FieldType

  case class ResultRep(node: OperatorNode) {
    def expressionCompiler(record: Rep[Record]): ExpressionCompiler =
      new ExpressionCompiler {
        override val context = PlanCompiler.context
        override def getField[T: PardisType](qualifier: Option[String], fieldName: String): Option[Rep[T]] = {
          val nameWithQualifier = qualifier.getOrElse("") + fieldName
          val field = if (fieldNames.contains(nameWithQualifier)) nameWithQualifier else fieldName
          fields.get(field).map {
            _ match {
              case SimpleField(tp, _) =>
                record_select(record, field)(typeRecord, tp).asInstanceOf[Rep[T]]
              case AggKeyField(tp, _) =>
                val rec = record.asInstanceOf[Rep[AGGRecord[Record]]]
                record_select(rec.key, field)(typeRecord, tp).asInstanceOf[Rep[T]]
              case AggResultField(index, tp, _) if tp == typeInt =>
                val rec = record.asInstanceOf[Rep[AGGRecord[Record]]]
                rec.aggs(unit(index)).toInt.asInstanceOf[Rep[T]]
              case AggResultField(index, tp, _) =>
                val rec = record.asInstanceOf[Rep[AGGRecord[Record]]]
                rec.aggs(unit(index)).asInstanceOf[Rep[T]]
            }
          }
        }
      }

    def fields: Map[String, FieldType] = Map(fieldNames zip fieldTypes: _*)

    // Expression compiler without input tuples
    val exp = new ExpressionCompiler {
      override val context = PlanCompiler.context
      override def getField[T: PardisType](qualifier: Option[String], fieldName: String): Option[Rep[T]] = ???
    }

    def fieldTypes: Seq[FieldType] = {
      import exp._
      node match {
        case ScanOpNode(table, _, _) =>
          table.attributes.toSeq.map { attr =>
            attr.dataType match {
              case tp if tp == DateType   => SimpleField(typeInt)
              case tp if tp == StringType => SimpleField(typeOptimalString)
              case tp: VarCharType        => SimpleField(typeOptimalString)
              case tp: PardisType[_]      => SimpleField(tp)
            }
          }
        case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == LeftSemiJoin =>
          left.resultType.fieldTypes
        case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == AntiJoin =>
          left.resultType.fieldTypes
        case AggOpNode(parent, aggs, gb, aggNames) =>
          val keyFields = gb.map { g =>
            val tp = g._1.pardisType
            AggKeyField(tp)
          }
          val aggFields = aggs.zipWithIndex.map { case (exp, index) => AggResultField(index, exp.pardisType) }
          keyFields ++ aggFields
        case UnionAllOpNode(_, _) =>
          ???
        case ProjectOpNode(parent, projNames, origFieldNames) =>
          // TODO: projectOp is not really being supported here. This should
          // look into the result of origFieldNames
          // parent.resultType.fieldTypes ++ origFieldNames.map { exp => AggKeyField(exp.pardisType) }
          parent.resultType.fieldTypes ++ projNames.map { parent.resultType.fields(_) }
        case ViewOpNode(parent, projNames, _) =>
          parent.resultType.fields.filterKeys { projNames.contains(_) }.values.toSeq
        case _ =>
          node.parents.map { _.resultType.fieldTypes }.reduce(_ ++ _)
      }
    }

    def fieldNames: Seq[String] = node match {
      case ScanOpNode(table, _, _) =>
        table.attributes.map { _.name }
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == LeftSemiJoin =>
        left.resultType.fieldNames
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == AntiJoin =>
        left.resultType.fieldNames
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) =>
        left.resultType.fieldNames.map { leftAlias + _ } ++ right.resultType.fieldNames.map { rightAlias + _ }
      case AggOpNode(parent, aggs, gb, aggNames) =>
        gb.map { _._2 } ++ aggNames
      case UnionAllOpNode(_, _) =>
        ???
      case ProjectOpNode(parent, projNames, _) =>
        parent.resultType.fieldNames ++ projNames
      case ViewOpNode(parent, projNames, _) =>
        projNames
      case _ =>
        node.parents.map { _.resultType.fieldNames }.reduce(_ ++ _)
    }

    def recordName: String = node match {
      case ScanOpNode(table, _, _) =>
        table.name
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == LeftSemiJoin =>
        left.resultType.recordName
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == AntiJoin =>
        left.resultType.recordName
      case _ if node.parents.length == 1 && node.parents(0).resultType.fieldNames == node.resultType.fieldNames =>
        node.parents(0).resultType.recordName
      case _ =>
        node.resultType.fieldNames.reduce(_ + "_" + _)
    }

    def pardisType: PardisType[Record] = node match {
      // TODO: Other ones?
      case ScanOpNode(table, _, _) =>
        dataRowTypeForName(recordName).asInstanceOf[TypeRep[Record]]
      case AggOpNode(_, _, gb, aggNames) =>
        // The AGGRecord is not necessarily a string
        val keyType = dataRowTypeForName(aggRecordNameForFields(gb.map { _._2 }))
        typeAGGRecord(keyType).asInstanceOf[TypeRep[Record]]
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == LeftSemiJoin =>
        left.resultType.pardisType
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == AntiJoin =>
        left.resultType.pardisType
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) =>
        typeDynamicCompositeRecord(left.resultType.pardisType, right.resultType.pardisType).asInstanceOf[TypeRep[Record]]
      case _ =>
        node.parents(0).resultType.pardisType
    }

    def getField[T: PardisType](record: Rep[Record], qualifier: Option[String], fieldName: String): Option[Rep[T]] = {
      node.resultType.expressionCompiler(record).getField(qualifier, fieldName)(implicitly[PardisType[T]])
    }

    def setField[T: PardisType](record: Rep[Record], name: String, rhs: Rep[T]): Rep[Unit] = node match {
      case AggOpNode(parent, _, gb, aggNames) =>
        val rec = record.asInstanceOf[Rep[AGGRecord[Record]]]
        gb.collectFirst {
          case (e, n) if n == name =>
            fieldSetter(rec.key, name, rhs)
        }.getOrElse {
          val index = aggNames.indexWhere { _ == name }
          if (index == -1) throw new Exception(s"Couldn't find key $name in the result result of $node")
          rec.aggs(unit(index)) = rhs.asInstanceOf[Rep[Double]]
        }
      case _ =>
        ???
    }

  }

}
