package ch.epfl.data
package dblab
package queryengine

import ch.epfl.data.sc.pardis.ir.Constant
import ch.epfl.data.sc.pardis.types.{PardisType, RecordType}
import ch.epfl.data.sc.pardis.types.PardisTypeImplicits._
import ch.epfl.data.sc.pardis.shallow.Record
import legobase.deep.LegoBaseQueryEngineExp
import ch.epfl.data.sc.pardis.deep.scalalib.ArrayOps

import schema._
import ch.epfl.data.dblab.storagemanager.{Loader => ShallowLoader}
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import frontend.parser.OperatorAST._
import frontend.parser._
import config.Config

/**
 * The main module for compiling query plans.
 *
 * @author Yannis Klonatos
 */
object PlanCompiler { this: LegoBaseQueryEngineExp =>
  type Rep[T] = LegoBaseQueryEngineExp#Rep[T]
  val context = new LegoBaseQueryEngineExp {}
  import context._

  //implicit def context: LegoBaseQueryEngineExp = new LegoBaseQueryEngineExp {}

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
      val qp = queryToOperator(operatorTree, schema)
      // Execute tree
      qp.open
      qp.next
    }

    import legobase.compiler._
    val settings = new Settings(List("-scala", "+no-sing-hm"))
    val validatedSettings = settings.validate()
    //    val compiler = new LegoCompiler(context, validatedSettings, schema, "ch.epfl.data.dblab.experimentation.runner.SimpleRunner") {
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

  // Query interpretation methods
  val decimalFormatter = new java.text.DecimalFormat("###.########");

  def recursiveGetField(n: String, alias: Option[String], t: Record, t2: Record = null): Any = {
    var stop = false
    val name = alias.getOrElse("") + n

    def searchRecordFields(rec: DynamicDataRow): Option[Any] = {
      var res: Option[Any] = None

      for (f <- rec.getNestedRecords() if !stop) {
        searchRecord(f) match {
          case Some(r) => res = Some(r); stop = true;
          case None    =>
        }
      }
      res
    }

    def searchRecord(rec: Record): Option[Any] = {
      /* Does a field exist with this name? If so immediately return it */
      val res = rec.getField(name)
      if (res.isDefined) res
      /* If not, is it a dynamic record on which we can look (and find) this attribute name?*/
      else if (rec.isInstanceOf[DynamicCompositeRecord[_, _]]) {
        val r = rec.asInstanceOf[DynamicCompositeRecord[_, _]]
        r.getField(name) match {
          case Some(f) => Some(f)
          case None    => None //searchRecordFields(rec)
        }
      } else if (rec.isInstanceOf[DynamicDataRow])
        searchRecordFields(rec.asInstanceOf[DynamicDataRow])
      else
        throw new Exception(s"$rec is not a DynamicDataRow record and does not have the field `$name`")
    }

    searchRecord(t) match {
      case Some(res) => res // rec.getField(name).get
      case None =>
        if (t2 == null) {
          if (alias.isDefined) recursiveGetField(n, None, t) // Last chance, search without alias
          else throw new Exception("BUG: Searched for field " + n + " in " + t + " and couldn't find it! (and no other record available to search in -- case 1/Alias:" + alias + ")")
        } else {
          searchRecord(t2) match {
            case None =>
              alias match {
                case Some(al) =>
                  recursiveGetField(n, None, t, t2) // Last chance, search with alias
                case None =>
                  throw new Exception("BUG: Searched for field " + n + " in " + t + " and couldn't find it! (and no other record available to search in -- case 2/Alias:" + alias + ")")
              }
            case Some(res) => res //rec.getField(name).get
          }
        }
    }
  }

  def printRecord(rec: Rep[Record], parent: OperatorNode, order: Seq[Expression] = scala.collection.immutable.Seq()): Rep[Unit] = {
    //System.out.println("Printing record...")
    // TODO: actually printing the fields nicely
    val fieldNames = if (order.isEmpty) {
      parent.fieldNames
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

    System.out.println(parent.fields)
    fieldNames.foreach { name =>
      val tpe = parent.fields.getOrElse(name, throw new Exception(s"Could not find field $name"))
      val f = parent.getField(rec, name, None)(tpe)
      tpe match {
        case IntType           => printf(unit("%d"), f)
        case CharType          => printf(unit("%c"), f)
        case DoubleType        => printf(unit("%.4f"), f) // TODO -- Precision should not be hardcoded
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

  // Generalize by passing a "getField" function (and therefore drop all the other parameters)
  case class ExpressionCompiler(node: OperatorNode, t1: Rep[Record] = null, node2: OperatorNode = null, t2: Rep[Record] = null) {
    implicit class ExpressionOps(e: Expression) {
      def pardisType: PardisType[_] = OperatorNodeOps.expressionToPardisType(e)

      def asRep[T: PardisType]: Rep[T] = parseExpression[T](e)
      def toRep: Rep[_] = parseExpression(e)(e.pardisType)
    }

    // def promoteNumbers[A: PardisType, B: PardisType](n1: Rep[A], n2: Rep[B]): (Rep[Any], Rep[Any]) = {
    //   ((implicitly[PardisType[A]], implicitly[PardisType[B]]) match {
    //     case (x, y) if x == y      => n1 -> n2
    //     case (IntType, DoubleType) => infix_asInstanceOf[Int](n1).toDouble -> n2
    //     //case (IntType, FloatType)    => n1.asInstanceOf[Int].toFloat -> n2
    //     //case (FloatType, DoubleType) => n1.asInstanceOf[Float].toDouble -> n2
    //     //case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
    //     case (DoubleType, IntType) => n1 -> infix_asInstanceOf[Int](n2).toDouble
    //     case (_, null)             => n1 -> n2
    //     case (null, _)             => n1 -> n2
    //     //case _ =>
    //     //n1.asInstanceOf[Double].toDouble -> n2.asInstanceOf[Double].toDouble // FIXME FIXME FIXME THIS SHOULD BE HAPPENING, TYPE INFERENCE BUG
    //     case (x, y)                => throw new Exception(s"Does not know how to find the common type for $x and $y")
    //   }).asInstanceOf[(Rep[Any], Rep[Any])]
    // }

    /**
     * This method receives two numbers and makes sure that both number have the same type.
     * If their type is different, it will upcast the number with lower type to the type
     * of the other number.
     */
    def promoteNumbers(e1: Expression, e2: Expression): (Rep[Any], Rep[Any]) = {
      val n1 = e1.toRep
      val n2 = e2.toRep
      ((e1.pardisType, e2.pardisType) match {
        case (x, y) if x == y      => n1 -> n2
        case (IntType, DoubleType) => infix_asInstanceOf[Int](n1).toDouble -> n2
        //case (IntType, FloatType)    => n1.asInstanceOf[Int].toFloat -> n2
        //case (FloatType, DoubleType) => n1.asInstanceOf[Float].toDouble -> n2
        //case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
        case (DoubleType, IntType) => n1 -> infix_asInstanceOf[Int](n2).toDouble
        case (_, null)             => n1 -> n2
        case (null, _)             => n1 -> n2
        //case _ =>
        //n1.asInstanceOf[Double].toDouble -> n2.asInstanceOf[Double].toDouble // FIXME FIXME FIXME THIS SHOULD BE HAPPENING, TYPE INFERENCE BUG
        case (x, y)                => throw new Exception(s"Does not know how to find the common type for $x and $y")
      }).asInstanceOf[(Rep[Any], Rep[Any])]
    }

    def addition[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] + y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] + y.asInstanceOf[Rep[Double]]
    }).asInstanceOf[Rep[A]]

    def product[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] * y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] * y.asInstanceOf[Rep[Double]]
    }).asInstanceOf[Rep[A]]

    def subtraction[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[A] = (implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]]
      case CharType   => x.asInstanceOf[Rep[Char]] - y.asInstanceOf[Rep[Char]]
    }).asInstanceOf[Rep[A]]

    def lessThan[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] < y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] < y.asInstanceOf[Rep[Double]]
    }

    def lessOrEqual[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] <= y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] <= y.asInstanceOf[Rep[Double]]
    }

    def greaterThan[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] > y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] > y.asInstanceOf[Rep[Double]]
    }

    def greaterOrEqual[A: TypeRep](x: Rep[_], y: Rep[_]): Rep[Boolean] = implicitly[TypeRep[A]] match {
      case IntType    => x.asInstanceOf[Rep[Int]] >= y.asInstanceOf[Rep[Int]]
      case DoubleType => x.asInstanceOf[Rep[Double]] >= y.asInstanceOf[Rep[Double]]
    }

    //
    def equals[A: TypeRep](x: Rep[A], y: Rep[A]): Rep[Boolean] = implicitly[TypeRep[A]] match {
      case IntType =>
        // TODO: this shouldn't be hardcoded, maybe it can even be inferred from the schema
        x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]] <= unit(1e-2) &&
          x.asInstanceOf[Rep[Int]] - y.asInstanceOf[Rep[Int]] >= unit(-1e-2)
      case DoubleType =>
        x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]] <= unit(1e-2) &&
          x.asInstanceOf[Rep[Double]] - y.asInstanceOf[Rep[Double]] >= unit(-1e-2)
      case other =>
        AllRepOps(x)(other).__==(y)
    }

    // // TODO: All the expression manipulation stuff could go into another class
    val subqueryInitializedMap = new scala.collection.mutable.HashMap[OperatorNode, Rep[SubquerySingleResult[Record]]]()
    def parseExpression[A: TypeRep](e: Expression): Rep[A] = {
      val result: Rep[_] = e match {
        // Literals
        case FieldIdent(qualifier, name, _) =>
          //recursiveGetField(name, qualifier, t, t2)
          val field = qualifier.getOrElse("") + name
          // TODO: better error handling. Abstract this functionality out of the expression compiler
          if (node != null && node2 != null) { // We are in a join
            // System.out.println(s"Node1 fields: ${node.fieldNames}")
            // System.out.println(s"Node2 fields: ${node2.fieldNames}")
            // System.out.println(s"Looking for $field")
            if (node.fieldNames.contains(name) || node.fieldNames.contains(field)) {
              node.getField(t1, name, qualifier)(e.pardisType)
            } else {
              node2.getField(t2, name, None)(e.pardisType)
            }
          } else {
            node.getField(t1, name, qualifier)(e.pardisType)
          }
        case DateLiteral(v)   => GenericEngine.parseDate(unit(v))
        case FloatLiteral(v)  => unit(v)
        case DoubleLiteral(v) => unit(v)
        case IntLiteral(v)    => unit(v)
        case StringLiteral(v) => GenericEngine.parseString(unit(v))
        case NullLiteral      => unit(null)
        case CharLiteral(v)   => unit(v)
        // Arithmetic Operators
        case Add(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          addition(e1, e2)(e1.tp)
        case Subtract(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          subtraction(e1, e2)(e1.tp)
        case Multiply(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          product(e1, e2)(e1.tp)
        // case Divide(left, right) =>
        //   // TODO: Check if this gives the correct result -- also fix asInstanceOf
        //   computeNumericExpression(left, right, { (x, y) =>
        //     if (y.isInstanceOf[Double])
        //       x.toInt / y.asInstanceOf[Double]
        //     else
        //       x.toInt / y.asInstanceOf[Int]
        //   }, t, t2)(left.tp, right.tp)
        // case UnaryMinus(expr) => computeNumericExpression(IntLiteral(-1), expr, (x, y) => x * y, t, t2)(expr.tp, expr.tp)
        // Logical Operators
        case Equals(left, right) =>
          val lhs = left.toRep
          val rhs = right.toRep
          equals(lhs, rhs)(lhs.tp)
        case NotEquals(left, right) =>
          val lhs = left.toRep
          val rhs = right.toRep
          !equals(lhs, rhs)(lhs.tp)
        case And(left, right) =>
          left.asRep[Boolean] && right.asRep[Boolean]
        case Or(left, right) =>
          left.asRep[Boolean] || right.asRep[Boolean]
        case GreaterOrEqual(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          greaterOrEqual(e1, e2)(e1.tp)
        case GreaterThan(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          greaterThan(e1, e2)(e1.tp)
        case LessOrEqual(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          lessOrEqual(e1, e2)(e1.tp)
        case LessThan(left, right) =>
          val (e1, e2) = promoteNumbers(left, right)
          lessThan(e1, e2)(e1.tp)
        case Not(expr) =>
          !expr.asRep[Boolean]
        // // SQL statements
        case Year(date) =>
          val d = date.toRep
          infix_asInstanceOf[Int](d) / unit(10000);
        case Substring(field, idx1, idx2) =>
          field.asRep[OptimalString].slice(idx1.asRep[Int], idx2.asRep[Int])
        case Like(field, value) =>
          val f = field.asRep[OptimalString]
          //val s = value.asRep[OptimalString]
          val str = value match {
            case StringLiteral(s: String) =>
              s
            case _ =>
              throw new Exception(s"Couldn't intrepret LIKE pattern: %str")
          }
          val delim = "%%"
          val v = GenericEngine.parseString(unit(str.replaceAll("%", "")))
          if (str.startsWith(delim) && str.endsWith(delim)) {
            f.containsSlice(v)
          } else if (str.endsWith(delim)) {
            f.startsWith(v)
          } else if (str.startsWith(delim)) {
            f.endsWith(v)
          } else {
            throw new Exception(s"Couldn't intrepret LIKE pattern: %str")
          }
        // case In(expr, values) => {
        //   val c = parseExpression(expr, t, t2)
        //   val v = values.map(parseExpression(_, t, t2))
        //   if (v.contains(c)) true
        //   else false
        // }
        // case StringConcat(str1, str2) =>
        //   // Todo move this to SC
        //   val left = parseExpression(str1, t, t2)
        //   val right = parseExpression[OptimalString](str2, t, t2)
        //   left + new String((right.data: scala.collection.mutable.ArrayOps[Byte]).map(_.toChar))
        case Case(cond, thenp, elsep) =>
          __ifThenElse(cond.asRep[Boolean], thenp.toRep, elsep.toRep)
        // // TODO -- Not good. Must be generalized. The extraction of the single field should go earlier in the pipeline.
        case GetSingleResult(parent: SubquerySingleResultNode) =>
          val p = subqueryInitializedMap.getOrElseUpdate(parent, convertOperator(parent).asInstanceOf[Rep[SubquerySingleResult[Record]]])
          val rec = p.getResult
          p.reset

          // TODO: hack
          if (parent.fieldNames.last == "__TOTAL_COUNT")
            parent.getField(rec, parent.fieldNames.head, None)(parent.fieldTypes.head)
          else if (parent.fieldNames.length > 1)
            throw new Exception(s"LegoBase BUG: Do not know how to extract single value from tuple with > 1 fields (${parent.fieldNames}).")
          // TODO: Generalize, this is a hack
          else
            parent.getField(rec, parent.fieldNames.last, None)(parent.fieldTypes.last)
      }
      result.asInstanceOf[Rep[A]]
    }
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
      val comp = ExpressionCompiler(leftParent, x, rightParent, y)
      import comp._
      joinCond.asRep[Boolean]
    }

    val leftFun = __lambda[Record, Any] { x =>
      val comp = ExpressionCompiler(leftParent, x)
      import comp._
      leftCond.toRep
    }

    val rightFun = __lambda[Record, Any] { x =>
      val comp = ExpressionCompiler(rightParent, x)
      import comp._
      rightCond.toRep
    }

    joinType match {
      case LeftSemiJoin =>
        __newLeftHashSemiJoinOp[Record, Record, Any](leftOp, rightOp)(joinFun)(leftFun)(rightFun)(
          leftParent.pardisType, rightParent.pardisType,
          OperatorNodeOps.expressionToPardisType(leftCond).asInstanceOf[PardisType[Any]]
        )
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
          implicitly[Manifest[Record]], leftParent.pardisType, rightParent.pardisType,
          OperatorNodeOps.expressionToPardisType(leftCond).asInstanceOf[PardisType[Any]]
        )
      case InnerJoin =>
        __newHashJoinOp[Record, Record, Any](leftOp, rightOp, unit(leftAlias), unit(rightAlias))(joinFun)(leftFun)(rightFun)(
          implicitly[Overloaded1], leftParent.pardisType, rightParent.pardisType,
          OperatorNodeOps.expressionToPardisType(leftCond).asInstanceOf[PardisType[Any]] // Uh oh...
        )
    }
  }

  def createAggOpOperator(node: OperatorNode, parentOp: OperatorNode, aggs: Seq[Expression], gb: Seq[(Expression, String)], aggNames: Seq[String]): Rep[Operator[Record]] = {
    System.out.println(s"AGGOP INFO: $aggs")
    System.out.println(gb)
    System.out.println(aggNames)

    val aggFuncs: Seq[Rep[(Record, Double) => Double]] = aggs.map { aggExpression =>
      __lambda[Record, Double, Double] { (t, currAgg) =>
        val expressionCompiler = ExpressionCompiler(parentOp, t)
        import expressionCompiler._
        val result: Rep[Double] = aggExpression match {
          case Sum(e) =>
            // TODO: is it necessary to promote the result of e.asRep?
            addition(currAgg, e.asRep[Double])
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
          val fname = unit(name)
          val tpe = OperatorNodeOps.expressionToPardisType(exp)
          val value = ExpressionCompiler(parentOp, t).parseExpression(exp)(tpe)
          Tuple2(fname, value)
      }: _*)
    }

    __newAggOp(
      convertOperator(parentOp),
      unit(aggs.length)
    )(grp)(aggFuncs: _*)(parentOp.pardisType, aggTpe)
  }

  def createSelectOperator(node: OperatorNode, parentOp: OperatorNode, cond: Expression): Rep[SelectOp[Record]] = {
    __newSelectOp(convertOperator(parentOp))(__lambda { t =>
      ExpressionCompiler(parentOp, t).parseExpression[Boolean](cond)
    })(node.pardisType)
  }

  def createMapOperator(node: OperatorNode, parentOp: OperatorNode, indices: Seq[(String, String)]): Rep[MapOp[Record]] = {
    val mapFuncs: Seq[Rep[Record => Unit]] = indices.map {
      case (idx1, idx2) =>
        __lambda[Record, Unit] { t =>
          parentOp.setField(t, idx1, parentOp.getField[Double](t, idx1, None) / parentOp.getField[Double](t, idx2, None))
        }
    }

    __newMapOp(convertOperator(parentOp))(mapFuncs: _*)(node.pardisType)
  }

  def createSortOperator(node: OperatorNode, parentOp: OperatorNode, orderBy: Seq[(Expression, OrderType)]): Rep[SortOp[Record]] = {
    __newSortOp(convertOperator(parentOp))(__lambda[Record, Record, Int] { (kv1, kv2) =>
      val expressions = orderBy.map { e =>
        val tp = OperatorNodeOps.expressionToPardisType(e._1)
        val k1 = ExpressionCompiler(parentOp, kv1).parseExpression(e._1)(tp)
        val k2 = ExpressionCompiler(parentOp, kv2).parseExpression(e._1)(tp)

        // Expression compiler without input tuples
        val expressionCompiler = ExpressionCompiler(parentOp)
        import expressionCompiler._

        val res = tp match {
          case i if i == typeInt           => subtraction[Int](k1, k2)
          // Multiply with 100 to account for very small differences (e.g. 0.02)
          case d if d == typeDouble        => product[Double](subtraction[Double](k1, k2), unit(100.0)).toInt
          case c if c == typeChar          => subtraction[Char](k1, k2).toInt
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
        __ifThenElse(acc __!= unit(0), acc, exp)
      }

      // Break arbitrarily
      __ifThenElse(result __!= unit(0), result, infix_hashCode(kv1) - infix_hashCode(kv2))
    }(parentOp.pardisType, parentOp.pardisType, typeInt))(node.pardisType)
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

    //implicit val tableType = dynamicDataRowRecordType("Print", List())
    __newPrintOp(convertOperator(parent))(__lambda { kv =>
      printRecord(kv, parent, finalProjs)
    }, unit(limit))(node.pardisType)
  }

  implicit class ExtraOperatorNodeOps(node: OperatorNode) extends OperatorNodeOps.OperatorASTNodeOps(node) {
    def pardisType: PardisType[Record] = node match {
      // TODO: Other ones?
      case ScanOpNode(table, _, _) =>
        dataRowTypeForName(node.recordName).asInstanceOf[TypeRep[Record]]
      case AggOpNode(_, _, gb, aggNames) =>
        // The AGGRecord is not necessarily a string
        val keyType = dataRowTypeForName(aggRecordNameForFields(gb.map { _._2 }))
        typeAGGRecord(keyType).asInstanceOf[TypeRep[Record]]
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == LeftSemiJoin =>
        left.pardisType
      case JoinOpNode(left, right, _, joinType, leftAlias, rightAlias) if joinType == AntiJoin =>
        left.pardisType
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) =>
        typeDynamicCompositeRecord(left.pardisType, right.pardisType).asInstanceOf[TypeRep[Record]]
      case _ =>
        node.parents(0).pardisType
    }

    // Maybe a better abstraction would be to have the field, fieldNames,
    // fieldTypes, and getField methods in a "ResultType" class
    // TODO: change result to an option type. It will make everything better. Trust me, I'm a comment
    def getField[T: PardisType](record: Rep[Record], name: String, qualifier: Option[String], forceQualifier: Boolean = false): Rep[T] = node match {
      // TODO: rethink the handling of qualifiers. Are they only being used in
      // joins? Maybe in most cases it will suffice to concatenate and forget about it?
      // TODO: better error handling
      case ScanOpNode(_, _, _) =>
        val field = qualifier.getOrElse("") + name
        if (fieldNames.contains(field)) {
          record_select(record, field)(record.tp, implicitly[TypeRep[T]])
        } else if (fieldNames.contains(name)) {
          // TODO: this is kind of a hack
          val f = if (forceQualifier) field else name
          record_select(record, f)(record.tp, implicitly[TypeRep[T]])
        } else {
          throw new Exception(s"Couldn't find key $field in the result result of ScanOpNode (fields: ${fieldNames}")
        }
      case AggOpNode(parent, _, gb, aggNames) =>
        val rec = record.asInstanceOf[Rep[AGGRecord[Record]]]
        val field = qualifier.getOrElse("") + name
        gb.collectFirst {
          case (e, n) if n == field =>
            // TODO: refactor this
            val tp = OperatorNodeOps.expressionToPardisType(e)
            record_select(rec.key, field)(typeRecord, tp).asInstanceOf[Rep[T]]
        }.getOrElse {
          val index = aggNames.indexWhere { _ == field }
          val result = if (index == -1) {
            if (qualifier.isDefined) {
              getField[T](record, name, None) // search without qualifier
            } else {
              throw new Exception(s"Couldn't find key $field in the result result of AggOpNode (fields: ${fieldNames}")
            }
          } else if (fields(field) == typeInt) {
            rec.aggs(unit(index)).toInt
          } else {
            rec.aggs(unit(index))
          }
          result.asInstanceOf[Rep[T]]
        }
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) =>
        val field = qualifier.getOrElse("") + name
        if (left.fieldNames.contains(field) || left.fieldNames.contains(name)) {
          left.getField[T](record, name, qualifier, forceQualifier = true)
        } else if (right.fieldNames.contains(name) || right.fieldNames.contains(field)) {
          right.getField[T](record, name, qualifier, forceQualifier = true)
        } else {
          throw new Exception(s"Couldn't find field $name in the result of $node")
        }

      // case ProjectOpNode(parent, projNames, origFieldNames) =>
      //   val field = qualifier.getOrElse("") + name
      //   if (projNames.contains(field)) {
      //     val index = projNames.indexOf(field)
      //     val projExpression = origFieldNames(index)
      //     ExpressionCompiler(parent, record).parseExpression(projExpression)
      //   } else {
      //     parent.getField(record, name, qualifier)
      //   }
      case _ if parents.length == 1 =>
        parents(0).getField(record, name, qualifier)
    }

    def setField[T: PardisType](record: Rep[Record], name: String, rhs: Rep[T]): Rep[Unit] = node match {
      // TODO: better error handling
      case ScanOpNode(_, _, _) =>
        ??? //fieldSetter(record, name, rhs)
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
      case JoinOpNode(left, right, _, _, leftAlias, rightAlias) =>
        ???
      case _ if parents.length == 1 =>
        ??? //parents(0).setField(record, name, rhs)
    }

  }

  def aggRecordNameForFields(fields: Seq[String]): String = {
    "AggKey" + fields.foldLeft("")(_ + "_" + _)
  }

  def convertOperator(node: OperatorNode): Rep[Operator[Record]] = {
    node match {
      case ScanOpNode(table, _, _) =>
        // TODO: maybe not use the shallow loader?
        //val records = ShallowLoader.loadUntypedTable(table)
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
        //p.reset
        __newSubquerySingleResult(p)(parent.pardisType)
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
}
