package ch.epfl.data
package dblab.legobase
package optimization
package monad

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import compiler._

class QueryMonadVerticalFusion(override val IR: LegoBaseExp) extends RuleBasedTransformer[LegoBaseExp](IR) {
  import IR._

  class T
  class S

  def zero[T: TypeRep]: Rep[T] = {
    // TODO generalize
    assert(typeRep[T] == DoubleType)
    unit(0.0).asInstanceOf[Rep[T]]
  }

  def add[T: TypeRep](a: Rep[T], b: Rep[T]): Rep[T] = {
    assert(typeRep[T] == DoubleType)
    (a.asInstanceOf[Rep[Double]] + b.asInstanceOf[Rep[Double]]).asInstanceOf[Rep[T]]
  }

  def div[T: TypeRep](a: Rep[T], b: Rep[Double]): Rep[T] = {
    assert(typeRep[T] == DoubleType)
    (a.asInstanceOf[Rep[Double]] / b).asInstanceOf[Rep[T]]
  }

  def div[T: TypeRep](a: Rep[T], b: Rep[Int])(implicit dum: DummyImplicit): Rep[T] = {
    assert(typeRep[T] == DoubleType)
    (a.asInstanceOf[Rep[Double]] / b).asInstanceOf[Rep[T]]
  }

  rewrite += rule {
    case QuerySum(Def(QueryMap(monad, f: Rep[T => S]))) =>
      implicit val typeT = f.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      implicit val typeS = f.tp.typeArguments(1).asInstanceOf[TypeRep[S]]
      // val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      // array_map(array, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      apply(monad).asInstanceOf[Rep[Query[T]]].foldLeft(zero[S])(__lambda { (acc, cur) =>
        val newCur = inlineFunction(f, cur).asInstanceOf[Rep[S]]
        add(acc, newCur)
      })

  }

  rewrite += rule {
    case QueryCount(Def(QueryMap(monad, f))) =>
      implicit val typeT = f.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      // val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      // array_map(array, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      apply(monad).asInstanceOf[Rep[Query[T]]].foldLeft(unit(0))(__lambda { (acc, cur) =>
        acc + unit(1)
      })
    // QueryCount(apply(monad))
  }

  rewrite += rule {
    case QueryCount(monad) =>
      implicit val typeT = monad.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      // val array = apply(monad).asInstanceOf[Rep[Array[Any]]]
      // array_map(array, f)(array.tp.typeArguments(0).asInstanceOf[TypeRep[Any]], f.tp.typeArguments(1).asInstanceOf[TypeRep[Any]])
      apply(monad).asInstanceOf[Rep[Query[T]]].foldLeft(unit(0))(__lambda { (acc, cur) =>
        acc + unit(1)
      })
    // QueryCount(apply(monad))
  }

  class K

  // rewrite += rule {
  //   case GroupedQueryMapValues(Def(QueryGroupBy(Def(QueryFilter(monad, pred)), par)), mapf) =>

  //     implicit val typeT = monad.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
  //     implicit val typeK = par.tp.typeArguments(1).asInstanceOf[TypeRep[K]]
  //     implicit val typeS = mapf.tp.typeArguments(1).asInstanceOf[TypeRep[S]]
  //     monad.asInstanceOf[Rep[Query[T]]].groupBy(par.asInstanceOf[Rep[T => K]]).mapValues(__lambda {
  //       l => mapf.asInstanceOf[Rep[Query[T] => S]](l.filter(pred))
  //     })
  // }

  rewrite += rule {
    case QueryGroupBy(Def(QueryFilter(monad, pred)), par) =>

      implicit val typeT = monad.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      implicit val typeK = par.tp.typeArguments(1).asInstanceOf[TypeRep[K]]
      apply(monad).asInstanceOf[Rep[Query[T]]].filteredGroupBy(pred.asInstanceOf[Rep[T => Boolean]],
        par.asInstanceOf[Rep[T => K]])
  }

  rewrite += rule {
    case QueryFoldLeft(Def(QueryFilter(monad, pred)), z, f) =>

      implicit val typeT = monad.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      implicit val typeS = z.tp.asInstanceOf[TypeRep[S]]
      val zTyped = z.asInstanceOf[Rep[S]]
      val predTyped = pred.asInstanceOf[Rep[T => Boolean]]
      val fTyped = f.asInstanceOf[Rep[(S, T) => S]]
      apply(monad).asInstanceOf[Rep[Query[T]]].foldLeft(zTyped)(__lambda { (acc, cur) =>
        val cond = inlineFunction(predTyped, cur)
        __ifThenElse(cond, {
          inlineFunction(fTyped, acc, cur)
        }, {
          acc
        })
      })
  }

  rewrite += rule {
    case QueryForeach(Def(QueryFilter(monad, pred)), f) =>
      implicit val typeT = monad.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      val predTyped = pred.asInstanceOf[Rep[T => Boolean]]
      val fTyped = f.asInstanceOf[Rep[T => Unit]]
      apply(monad).asInstanceOf[Rep[Query[T]]].foreach(__lambda { elem =>
        val cond = inlineFunction(predTyped, elem)
        __ifThenElse(cond, {
          inlineFunction(fTyped, elem)
        }, {
          unit()
        })
      })
  }

  rewrite += rule {
    case QueryAvg(monadMap @ Def(QueryMap(monad, f: Rep[T => S]))) =>
      implicit val typeT = f.tp.typeArguments(0).asInstanceOf[TypeRep[T]]
      implicit val typeS = f.tp.typeArguments(1).asInstanceOf[TypeRep[S]]
      // val result = monad.asInstanceOf[Rep[Query[T]]].foldLeft(Tuple2(zero[S], zero[Double]))(__lambda { (acc, cur) =>
      //   val newCur = inlineFunction(f, cur).asInstanceOf[Rep[S]]
      //   val sum = add(acc._1, newCur)
      //   val count = acc._2 + unit(1.0)
      //   Tuple2(sum, count)
      // })
      // div(result._1, result._2)
      // HACK!!
      val monadMapTyped = apply(monadMap).asInstanceOf[Rep[Query[Double]]]
      div(monadMapTyped.sum, monadMapTyped.count)
  }

}
