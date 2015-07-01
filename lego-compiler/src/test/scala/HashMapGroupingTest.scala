package ch.epfl.data
package dblab.legobase
package quasi

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import optimization._
import tpch._
import compiler.TreeDumper
import deep._
import sc.pardis.optimization._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.quasi.anf._
import schema._

class HashMapGroupingTest extends FlatSpec with ShouldMatchers {

  implicit val IR = new LoweringLegoBase {
  }

  import IR.Predef._

  val schema = TPCHSchema.getSchema(Config.datapath, 0.1)

  def transform[T: TypeRep](block: Block[T]): Block[T] = {
    val pipeline = new TransformerPipeline()
    pipeline += LBLowering(false)
    pipeline += ParameterPromotion
    pipeline += DCE
    pipeline += PartiallyEvaluate

    pipeline += new HashMapGrouping(IR, schema)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += new c.BlockFlattening(IR)
    pipeline += TreeDumper(true)

    pipeline(IR)(block)
  }

  "hash join pattern" should "work" in {
    val exp = {
      IR.reifyBlock {
        val lineitemArray = {
          import IR._
          __newArray[LINEITEMRecord](unit(100000))
        }
        val ordersArray = {
          import IR._
          __newArray[ORDERSRecord](unit(1000))
        }
        val multiMap = {
          import IR._
          __newMultiMap[Int, ORDERSRecord]()
        }

        dsl"""
        var ordersIndex = 0
        while(ordersIndex < $ordersArray.length) {
          val elem = $ordersArray(ordersIndex)
          $multiMap.addBinding(elem.O_ORDERKEY, elem)
          ordersIndex += 1
        }
        var lineitemIndex = 0
        while(lineitemIndex < $lineitemArray.length) {
          val elem = $lineitemArray(lineitemIndex)
          val setOption = $multiMap.get(elem.L_ORDERKEY)
          if(setOption.nonEmpty) {
            val tmpBuffer = setOption.get
            tmpBuffer foreach { bufElem =>
              if (bufElem.O_ORDERKEY == elem.L_ORDERKEY) {
                println(elem)
                println(bufElem)
              }
            }
          }
          lineitemIndex += 1
        }
        """
      }
    }
    val newExp = transform(exp)
    newExp match {
      // TODO needs more support from the quasi-quote engine
      case dsl"""__block{ 
        var lineitemIndex = 0
        val lineitemArray = ($lineitemArray: Array[LINEITEMRecord])
        val ordersArray = ($ordersArray: Array[ORDERSRecord])
        while(lineitemIndex < lineitemArray.length) {
          val elem = lineitemArray(lineitemIndex)
          val key = __struct_field[Int](elem, "L_ORDERKEY")
          val relem = ordersArray(key)
          val rkey = __struct_field[Int](relem, "O_ORDERKEY")
          val equalityCheck = rkey == key
          // TODO needs println to be quasi-lifted
          // if(rkey == key) {
          //   println(elem)
          //   println(relem)
          // } 
          lineitemIndex += 1
        }
      }""" =>
        assert(lineitemArray.tp.typeArguments(0).name == "LINEITEMRecord")
        assert(ordersArray.tp.typeArguments(0).name == "ORDERSRecord")
    }
  }
}
