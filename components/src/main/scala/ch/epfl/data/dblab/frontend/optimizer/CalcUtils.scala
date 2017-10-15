package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.parser.{CalcAST, SQLAST}
import ch.epfl.data.dblab.frontend.parser.SQLAST._
import ch.epfl.data.dblab.schema.{Attribute, DateType, Table}
import ch.epfl.data.sc.pardis.types._
import ch.epfl.data.dblab.frontend.optimizer.CalcOptimizer._
/**
 * @author Parand Alizadeh
 */

object CalcUtils {


  def tableHasStream(table: Table): Boolean = {
    ???
  }
  def applyListAsFunction[A](theta: List[(A,A)], defau: A, x: A): A ={
    def g[B](x: B, y:B) = {
      if(x == y)
        List(y)
      else
        List()
    }

    val x2 = theta.map(x => g(x._1, x._2)).foldLeft(List.empty[A])((acc, cur) => acc ::: cur)
    if(x2.length == 0)
      defau
    else if(x2.length == 1)
      x2.head
    else
      throw new Exception

  }
  def applyIfPresent[A](theta : List[(A, A)])(x: A): A = {
    applyListAsFunction(theta, x, x)
  }

  def renameVarsArithmetic(mapping: List[(VarT, VarT)], x : CalcValue): CalcExpr = {
    ???
  }
  def renameVars(mappings: List[(VarT, VarT)], expr: CalcExpr): CalcExpr = {
    val remapOne = applyIfPresent(mappings)
    val remap = ???
    ???
  }


    //TODO
  def reduceAssoc[A,B](l: List[(A,B)]): List[(A, List[B])] = {
    l.foldRight(List.empty[(A, List[B])])((acc, cur) => {
      ???
    })
  }

  def toVarT(atr: Attribute): VarT = {
    VarT(atr.name, atr.dataType)
  }

  def toAttribute(v: VarT): Attribute = {
    Attribute(v.name, v.tp)
  }
  def extractRenamings(scope: List[VarT], schema: List[VarT], calcExpr: CalcExpr): (List[(VarT, VarT)], CalcExpr) = {
    val (rawmappings, exprTerms) = prodList(calcExpr).foldLeft((List.empty[(VarT, VarT)], List.empty[CalcExpr]))((acc, cur) => {
      cur match {
        case Lift(v1, CalcValue(ArithVar(v2))) if (scope.contains(v2) && schema.contains(v1)) => ((v1, v2) :: acc._1, acc._2)
        case Lift(v1, CalcValue(ArithVar(v2))) if (scope.contains(v1) && scope.contains(v2)) => (acc._1, acc._2 ++ List(Cmp(Eq, ArithVar(v1), ArithVar(v2))))
        case _ => (acc._1, acc._2 ++ List(cur))
      }
    })
   val (mappings, mappingCondition) = reduceAssoc(rawmappings).map(cur => cur._2 match {
     case List() => throw new Exception("BUG: reduce returned an empty list")
     case x::rest => ((cur._1, x), CalcProd(rest.map(y => Cmp(Eq, ArithVar(x), ArithVar(y)))))
   }).unzip

   def fixSchemas(expr: CalcExpr): CalcExpr = {
     def leaf(x: CalcExpr): CalcExpr = {
       x match {
         case AggSum(gbvars, subexp) => {
           val newsubexp = fixSchemas(subexp)
           AggSum(gbvars.toSet.intersect(schemaOfExpression(newsubexp)._2.toSet).toList, newsubexp)
         }
         case Lift(var1, CalcValue(ArithVar(var2))) if(var1 == var2) => CalcOne
         case Lift(var1, subexp) => Lift(var1, fixSchemas(subexp))
         case _ => x
       }
     }
     fold(sumGeneral, prodGeneral, negGeneral, leaf, expr)
   }
 ???

  }
  def prodList(calcExpr: CalcExpr): List[CalcExpr] = {
    calcExpr match {
      case CalcProd(l) => l
      case _           => List(calcExpr)
    }
  }
  def extractDomains(scope: List[VarT], calcExpr: CalcExpr): (CalcExpr, CalcExpr) = {
    val schema = schemaOfExpression(calcExpr)._2

    //TODO optimize

    val res = prodList(calcExpr).foldLeft[(CalcExpr, CalcExpr)]((CalcOne, CalcOne))((acc, cur) => {
      cur match {
        case DomainDelta(_) => (CalcProd(List(acc._1, cur)), acc._2)
        case _              => (acc._1, CalcProd(List(acc._2, cur)))
      }

    })
    res
  }

  def eventVars(e: EventT): List[VarT] = {
    e match {
      case InsertEvent(rel)                          => rel.attributes.map(toVarT)
      case DeleteEvent(rel)                          => rel.attributes.map(toVarT)
      case BatchUpdate(_)                            => List()
      case CorrectiveUpdate(_, ivars, ovars, v, upe) => (ivars ++ ovars ++ List(v) ++ eventVars(upe))
      case SystemInitializedEvent()                  => List()
    }
  }
  def delta(leaf: (CalcExpr => CalcExpr))(e: CalcExpr): CalcExpr = {
    e match {
      case CalcSum(l)       => CalcSum(l.map(delta(leaf)_))
      case CalcProd(List()) => CalcZero
      case CalcProd(x :: l) => {
        val r = List(CalcProd(delta(leaf)(x) :: l), CalcProd(List(CalcSum(List(x, delta(leaf)(x))), delta(leaf)(CalcProd(l)))))
        CalcSum(r)
      }
      case CalcNeg(ex) => CalcNeg(delta(leaf)(ex))
      case _           => leaf(e)
    }
  }
  def singleton(cols: List[(VarT, CalcExpr)], multiplicity: CalcExpr = CalcOne): CalcExpr = {
    CalcProd(multiplicity :: cols.map(x => Lift(x._1, x._2)))
  }

  def deltaOfExpr(deltaEvent: EventT, expr: CalcExpr): CalcExpr = {

    def templateBatchDeltaOfRel(deltareln: String)(reln: String, relvars: List[VarT]): CalcExpr = {
      if (deltareln == reln)
        DeltaRel(reln, relvars)
      else
        CalcZero
    }

    def templateDeltaOfRel(applySign: (CalcExpr => CalcExpr), deltaRel: Table)(reln: String, relv: List[VarT]) = {
      if (deltaRel.name == reln) {
        if (relv.length != deltaRel.attributes.map(toVarT).length)
          throw new Exception
        else {
          val definitionTerms = singleton(relv.zip(deltaRel.attributes.map(toVarT).map(x => CalcValue(ArithVar(x)))))
          applySign(definitionTerms)
        }
      } else
        CalcZero
    }
    val emptyDeltaOfRel = (n: String, v: List[VarT]) => CalcZero

    val errorDeltaOfExt = (ex: CalcAST.External) => {
      throw new Exception("Cannot take delta of an external")
    }

    def pos(expr: CalcExpr) = expr
    def neg(expr: CalcExpr) = CalcNeg(expr)

    val deltaOfRel = deltaEvent match {
      case InsertEvent(deltaRel) => templateDeltaOfRel(pos, deltaRel)_
      case DeleteEvent(deltaRel) => templateDeltaOfRel(neg, deltaRel)_
      case BatchUpdate(deltaReln) => templateBatchDeltaOfRel(deltaReln)_
      case CorrectiveUpdate(deltaExtName, ivars, ovars, value, _) => emptyDeltaOfRel
      case _ => throw new Exception("Error: Can not take delta of a non relation event")

    }

    val deltaOfExt = deltaEvent match {
      case InsertEvent(deltaRel)  => errorDeltaOfExt
      case DeleteEvent(deltaRel)  => errorDeltaOfExt
      case BatchUpdate(deltaReln) => errorDeltaOfExt
      case CorrectiveUpdate(deltaExtName, ivars, ovars, value, _) => {
        val res = (e: External) => {
          if (e.name == deltaExtName)
            singleton(e.outs.zip(ovars.map(x => CalcValue(ArithVar(x)))), CalcProd(CalcValue(ArithVar(value)) :: (e.inps.zip(ivars)).map(x => Cmp(Eq, ArithVar(x._1), ArithVar(x._2)))))
          else
            CalcZero

        }
        res
      }
      case _ => throw new Exception("Error: Can not take delta of a non relation event")

    }

    def leaf(calcExpr: CalcExpr): CalcExpr = {
      calcExpr match {
        case CalcValue(_) => CalcZero
        case AggSum(gbvars, subt) => {
          val subtdelta = deltaOfExpr(deltaEvent, subt)
          val ins = schemaOfExpression(subt)._1
          val outs = schemaOfExpression(subtdelta)._2
          val newgbs = gbvars.toSet.union(ins.toSet.intersect(outs.toSet)).toList
          if (subtdelta.equals(CalcZero))
            CalcZero
          else
            AggSum(newgbs, subtdelta)
        }
        case Rel(_, name, vars, _)            => deltaOfRel(name, vars)
        case DeltaRel(_, _)                   => CalcZero
        case External(name, ins, outs, tp, m) => deltaOfExt(External(name, ins, outs, tp, m))
        case Cmp(_, _, _)                     => CalcZero
        case CmpOrList(_, _)                  => CalcZero
        case Lift(vr, subt) => {
          val deltaterm = deltaOfExpr(deltaEvent, subt)
          if (deltaterm.equals(CalcZero))
            CalcZero
          else {
            val scope = eventVars(deltaEvent)
            val (deltalhs, deltarhs) = extractDomains(scope, deltaterm)
            CalcProd(List(deltalhs, CalcSum(List(Lift(vr, CalcSum(List(subt, deltarhs))), CalcNeg(Lift(vr, subt))))))
          }
        }
        case CalcAST.Exists(subt) => {
          val deltaTerm = deltaOfExpr(deltaEvent, subt)
          if (deltaTerm == CalcZero)
            CalcZero
          else {
            val scope = eventVars(deltaEvent)
            val (deltalhs, deltarhs) = extractDomains(scope, deltaTerm)
            CalcProd(List(deltalhs, CalcSum(List(CalcAST.Exists(CalcSum(List(subt, deltarhs))), CalcNeg(CalcAST.Exists(subt))))))
          }
        }

      }
    }
    delta(leaf)(expr)

  }

  def deltaRelsOfExpression(expr: CalcExpr): List[String] = {
    def leaf(calcExpr: CalcExpr): List[String] = {
      calcExpr match {
        case CalcValue(_)                  => List()
        case External(_, _, _, _, None)    => List()
        case External(_, _, _, _, Some(s)) => deltaRelsOfExpression(s)
        case AggSum(_, e)                  => deltaRelsOfExpression(e)
        case Rel(_, _, _, _)               => List()
        case DeltaRel(rn, _)               => List(rn)
        case Cmp(_, _, _)                  => List()
        case CmpOrList(_, _)               => List()
        case Lift(_, e)                    => deltaRelsOfExpression(e)
        case CalcAST.Exists(e)             => deltaRelsOfExpression(e)

      }
    }
    def neg(x: List[String]): List[String] = x

    fold(multiunion, multiunion, neg, leaf, expr)

  }
  def getCalcFiles(folder: String): List[String] = {
    val f = new java.io.File(folder)
    if (!f.exists) {
      throw new Exception(s"$f does not exist!")
    } else
      f.listFiles.filter(_.getName().endsWith(".calc")).map(folder + "/" + _.getName).toList
  }

  def escalateType(a: Tpe, b: Tpe): Tpe = {
    (a, b) match {
      case (at, bt) if (at.equals(bt)) => at
      case (t, AnyType)                => t
      case (AnyType, t)                => t
      case (IntType, BooleanType)      => IntType
      case (BooleanType, IntType)      => IntType
      case (IntType, FloatType)        => FloatType
      case (FloatType, IntType)        => FloatType
      case _                           => AnyType

    }
  }

  def escalateTypeList(list: List[Tpe]): Tpe = {
    if (list.isEmpty)
      IntType
    list.tail.foldLeft(list.head)((acc, cur) => escalateType(acc, cur))
  }

  def typeOfConst(expression: SQLAST.LiteralExpression): Tpe = {
    expression match {
      case IntLiteral(_)    => IntType
      case DoubleLiteral(_) => DoubleType
      case FloatLiteral(_)  => FloatType
      case StringLiteral(_) => StringType
      case DateLiteral(_)   => DateType
      case CharLiteral(_)   => CharType
      case _                => AnyType
    }
  }

  def typeOfValue(expr: ArithExpr): Tpe = {
    def neg(tpe: Tpe): Tpe = {
      tpe match {
        case IntType   => tpe
        case FloatType => tpe
        case _         => throw new Exception
      }
    }
    expr match {
      case ArithSum(sum)          => escalateTypeList(sum.map(x => typeOfValue(x)))
      case ArithProd(prod)        => escalateTypeList(prod.map(x => typeOfValue(x)))
      case ArithNeg(n)            => neg(typeOfValue(n))
      case ArithConst(c)          => typeOfConst(c)
      case ArithVar(t)            => t.tp
      case ArithFunc(_, inps, tp) => tp
    }

  }

  def typeOfExpression(expr: CalcExpr): Tpe = {
    def leaf(calcExpr: CalcExpr): Tpe = {
      calcExpr match {
        case CalcValue(v)            => typeOfValue(v)
        case External(_, _, _, e, _) => e
        case AggSum(_, sub)          => typeOfExpression(sub)
        case Rel(_, _, _, _)         => IntType
        case Cmp(_, _, _)            => IntType
        case CmpOrList(_, _)         => IntType
        case Lift(_, _)              => IntType
        case CalcAST.Exists(_)       => IntType
        case _                       => IntType
      }
    }

    def neg(tpe: Tpe): Tpe = tpe
    fold(escalateTypeList, escalateTypeList, neg, leaf, expr)
  }

  def multiunion(list: List[List[String]]): List[String] = {
    list.foldLeft(List.empty[String])((acc, cur) => acc.toSet.union(cur.toSet).toList)
  }
  def relsOfExpr(expr: CalcExpr): List[String] = {

    def leaf(calcExpr: CalcExpr): List[String] = {
      calcExpr match {
        case CalcValue(_)                  => List()
        case External(_, _, _, _, None)    => List()
        case External(_, _, _, _, Some(e)) => relsOfExpr(e)
        case AggSum(_, sub)                => relsOfExpr(sub)
        case Rel(_, rn, _, _)              => List(rn)
        case CmpOrList(_, _)               => List()
        case Lift(_, sub)                  => relsOfExpr(sub)
        case CalcAST.Exists(sub)           => relsOfExpr(sub)
        case Cmp(_, _, _)                  => List()
      }
    }

    def neg(list: List[String]): List[String] = list
    fold(multiunion, multiunion, neg, leaf, expr)
  }

  def exprHasDeltaRels(expr: CalcExpr): Boolean = {
    expr match {
      case CalcProd(pl)               => pl.map(x => exprHasDeltaRels(x)).exists(x => x)
      case CalcSum(sl)                => sl.map(x => exprHasDeltaRels(x)).forall(x => x)
      case CalcNeg(e)                 => exprHasDeltaRels(e)
      case CalcValue(_)               => false
      case Cmp(_, _, _)               => false
      case CmpOrList(_, _)            => false
      case Rel(_, _, _, _)            => false
      case AggSum(_, sub)             => exprHasDeltaRels(sub)
      case Lift(_, sub)               => exprHasDeltaRels(sub)
      case CalcAST.Exists(sub)        => exprHasDeltaRels(sub)
      case External(name, _, _, _, _) => (name.contains("DELTA") || name.contains("DOMAIN"))
    }
  }

  def exprHasLowCardinality(expr: CalcExpr): Boolean = exprHasDeltaRels(expr)

  def exprHasHighCardinality(expr: CalcExpr): Boolean = {
    (!exprHasLowCardinality(expr)) && (relsOfExpr(expr).size != 0)
  }
  /**
   * Determine whether two expressions safely commute (in a product).
   *
   * param scope  (optional) The scope in which the expression is evaluated
   * (having a subset of the scope may produce false negatives)
   * param e1     The left hand side expression
   * param e2     The right hand side expression
   * return       true if and only if e1 * e2 = e2 * e1
   */

  def commutes(expr1: CalcExpr, expr2: CalcExpr): Boolean = {
    val (_, ovars1) = schemaOfExpression(expr1)
    val (ivars2, _) = schemaOfExpression(expr2)

    (ivars2.toSet.intersect(ovars1.toSet).isEmpty && !((exprHasLowCardinality(expr1)) && exprHasHighCardinality(expr2)))

  }

  //Ring Fold
  def fold[A](sumFun: List[A] => A, prodFun: List[A] => A, negFun: A => A, leafFun: CalcExpr => A, expr: CalcExpr): A = {

    def rcr(expr: CalcExpr): A = {
      fold(sumFun, prodFun, negFun, leafFun, expr)
    }
    expr match {
      case CalcSum(list)  => sumFun(list.map(x => rcr(x)))
      case CalcProd(list) => prodFun(list.map(x => rcr(x)))
      case CalcNeg(e)     => negFun(rcr(e))
      case _ => {
        leafFun(expr)
      }
    }
  }

  def foldOfVars(sum: List[List[VarT]] => List[VarT], prod: List[List[VarT]] => List[VarT], neg: List[VarT] => List[VarT], leaf: ArithExpr => List[VarT], expr: ArithExpr): List[VarT] = {

    def rcr(expr: ArithExpr): List[VarT] = {

      foldOfVars(sum, prod, neg, leaf, expr)
    }

    expr match {
      case ArithSum(list)  => sum(list.map(x => rcr(x)))
      case ArithProd(list) => prod(list.map(x => rcr(x)))
      case ArithNeg(e)     => neg(rcr(e))
      case _               => leaf(expr)
    }
  }

  def varsOfValue(expr: ArithExpr): List[VarT] = {
    def multiunion(list: List[List[VarT]]): List[VarT] = {
      list.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
    }
    def leaf(arithExpr: ArithExpr): List[VarT] = {
      arithExpr match {
        case ArithConst(_)       => List()
        case ArithVar(v)         => List(v)
        case ArithFunc(_, tr, _) => multiunion(tr.map(x => varsOfValue(x)))
      }
    }
    foldOfVars(multiunion, multiunion, (x => x), leaf, expr)

  }
  def schemaOfExpression(expr: CalcExpr): (List[VarT], List[VarT]) = {

    def sum(sumlist: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {

      val (ivars, ovars) = sumlist.unzip
      val oldivars = ivars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val oldovars = ovars.foldLeft(List.empty[VarT])((acc, cur) => acc.toSet.union(cur.toSet).toList)
      val newivars = oldovars.toSet.diff(ovars.foldLeft(Set.empty[VarT])((acc, cur) => acc.intersect(cur.toSet))).toList

      (oldivars.toSet.union(newivars.toSet).toList, oldovars.toSet.diff(newivars.toSet).toList)
    }

    def prod(prodList: List[(List[VarT], List[VarT])]): (List[VarT], List[VarT]) = {

      prodList.foldLeft((List.empty[VarT], List.empty[VarT]))((oldvars, newvars) => ((oldvars._1.toSet).union(newvars._1.toSet.diff(oldvars._2.toSet)).toList, (oldvars._2.toSet.union(newvars._2.toSet)).diff(oldvars._1.toSet).toList))

    }

    def negSch(varTs: (List[VarT], List[VarT])): (List[VarT], List[VarT]) = { varTs }

    def leafSch(calcExpr: CalcExpr): (List[VarT], List[VarT]) = {

      def lift(target: VarT, expr: CalcExpr): (List[VarT], List[VarT]) = {
        val (ivars, ovars) = schemaOfExpression(expr)
        (ivars.toSet.union(ovars.toSet).toList, List(target))
      }

      def aggsum(gbvars: List[VarT], subexp: CalcExpr): (List[VarT], List[VarT]) = {

        val (ivars, ovars) = schemaOfExpression(subexp)
        val trimmedGbVars = ovars.toSet.intersect(gbvars.toSet).toList
        if (!(trimmedGbVars.equals(gbvars)))
          throw new Exception
        else
          (ivars, gbvars)
      }

      calcExpr match {
        case CalcValue(v)                   => (varsOfValue(v), List())
        case External(_, eins, eouts, _, _) => (eins, eouts)
        case AggSum(gbvars, subexp)         => { aggsum(gbvars, subexp) }
        case Rel("Rel", _, rvars, _)        => (List(), rvars)
        case Cmp(_, v1, v2)                 => (varsOfValue(v1).toSet.union(varsOfValue(v2).toSet).toList, List())
        case CmpOrList(v, _)                => (varsOfValue(v), List())
        case Lift(target, subexpr)          => lift(target, subexpr)
        case CalcAST.Exists(expr)           => schemaOfExpression(expr)
        // case _                              => (List(), List())

      }
    }
    fold(sum, prod, negSch, leafSch, expr)

  }
  def rewrite(expression: CalcExpr, sumFunc: List[CalcExpr] => CalcExpr, prodFunc: List[CalcExpr] => CalcExpr, negFunc: CalcExpr => CalcExpr, leafFunc: CalcExpr => CalcExpr): CalcExpr = {
    expression match {
      case CalcQuery(name, expr) => CalcQuery(name, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case CalcProd(list)        => prodFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => acc :+ rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc)))
      case CalcSum(list)         => sumFunc(list.foldLeft(List.empty[CalcExpr])((acc, cur) => acc :+ rewrite(cur, sumFunc, prodFunc, negFunc, leafFunc)))
      case CalcNeg(expr)         => rewrite(negFunc(expr), sumFunc, prodFunc, negFunc, leafFunc)
      case AggSum(t, expr)       => AggSum(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case External(name, inps, outs, tp, meta) => meta match {
        case Some(expr) => External(name, inps, outs, tp, Some(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc)))
        case None       => expression
      }
      case Lift(t, expr)        => Lift(t, rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case CalcAST.Exists(expr) => CalcAST.Exists(rewrite(expr, sumFunc, prodFunc, negFunc, leafFunc))
      case _                    => leafFunc(expression)
    }
  }

}