package ch.epfl.data
package dblab
package transformers

import deep.newqq._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.sc.pardis
import pardis.ir.Constant
import squid.utils._
import squid.ir.RewriteAbort
import QueryEngineIR.Sqd
import Sqd.Predef._
import Sqd.Quasicodes._
import pardis.ir.ExpressionSymbol
import squid.ir.SimpleRuleBasedTransformer
import squid.ir.TopDownTransformer
import sc.pardis.optimization.TransformerHandler
import sc.pardis.ir.Base
import sc.pardis.types.PardisType

object SingletonHashMapToValueTransformer2 extends SimpleRuleBasedTransformer with TopDownTransformer with Sqd.SelfTransformer with TransformerHandler {
  QueryEngineIR

  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    //base debugFor
    this.transform(block).asInstanceOf[context.Block[T]]
  }

  rewrite {

    case ir"val $hm: HashMap[$kt,$vt] = new HashMap(); $body: $bt" =>
      println(s"<< Running rwr code! >>\n\thm = $hm\n\tbody = $body")

      var uniqueUse = Option.empty[(kt.Typ, IR[vt.Typ, {}])]

      def giveUp: IR[Nothing, {}] = throw new RewriteAbort

      val body2 = body rewrite {
        // FIXME ClassDef exc:
        //case ir"$$hm.getOrElseUpdate(${ Const(key) }, $value)" if uniqueUse.isEmpty =>
        case ir"$$hm.getOrElseUpdate(${ key }, $value)" if uniqueUse.isEmpty && key.rep.isInstanceOf[Constant[_]] =>
          val closedValue = value.asInstanceOf[IR[vt.Typ, {}]] // TODO actually check no holes -- TODO a nice way to runtime-cast contexts!
          uniqueUse = Some(key.rep.asInstanceOf[Constant[kt.Typ]].underlying -> closedValue)
          ir"$$value: $vt"

        case ir"$$hm.getOrElseUpdate($k, $v)" if uniqueUse.nonEmpty => giveUp

        case ir"$$hm.size" => ir"1"

        case ir"$$hm foreach ($f: (($$kt,$$vt)) => $rt)" =>
          // TODO warn inferred Nothing!!

          ir"$f($$key: $kt, $$value: $vt); ()"
        // TODO inline:
        //ir"${ QueryEngineIR.Sqd.inline(f, ir"($$key: $kt, $$value: $vt)") }; ()"

      }

      println("body2: " + body2)

      //val body3 = body2 subs ('hm -> giveUp)
      val body3 = body2 subs ('hm -> {
        System.out.println(s"Found remaining usage of `hm` in $body2")
        giveUp
      })

      ir"val value = ${uniqueUse map (_._2) getOrElse giveUp}; val key = ${Const(uniqueUse.get._1)}; $body3"
  }

  // TODO use as test:
  /*
    rewrite {
      
      case ir"val $hm: HashMap[$kt,$vt] = $init; $body: $bt" =>
        println(s"Running rwr code!\n\thm = $hm\n\tinit = $init\n\tbody = $body")

        var uniqueUse = Option.empty[(kt.Typ, IR[vt.Typ, body.Ctx])]

        def giveUp: IR[Nothing, {}] = throw new RewriteAbort

        val body2 = body rewrite {
          // FIXME ClassDef exc:
          //case ir"$$hm.getOrElseUpdate(${ Const(key) }, $value)" if uniqueUse.isEmpty =>
          case ir"$$hm.getOrElseUpdate(${ key }, $value)" if uniqueUse.isEmpty && key.rep.isInstanceOf[Constant[_]] =>
            println(key)
            ir"$value"
          case ir"$$hm.getOrElseUpdate($k, $v)" if uniqueUse.isEmpty => giveUp
        }

        println("body2: " + body2)

        //ir"???"
        //body2
        ir"val hm = $init; $body2"
    }
    */

}