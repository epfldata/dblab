package ch.epfl.data
package dblab
package legobase
package deep
package quasi

import org.scalatest.{ FlatSpec, ShouldMatchers }
import prettyprinter._
import transformers._
import transformers.monad._
import dblab.experimentation.tpch._
import sc.pardis.optimization._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.quasi.anf._
import schema._
import dblab.queryengine.monad._
import legobase.deep.LegoBaseQueryEngineExp
import config.Config

class QueryMonadCPSLoweringTest extends FlatSpec with ShouldMatchers {

  implicit val IR = new LegoBaseQueryEngineExp {
  }

  import IR.Predef._

  val schema = TPCHSchema.getSchema(Config.datapath, 0.1)

  def transform[T: TypeRep](block: Block[T]): Block[T] = {
    val pipeline = new TransformerPipeline()
    pipeline += RecordLowering(false)
    pipeline += ParameterPromotion
    pipeline += DCE
    pipeline += PartiallyEvaluate

    pipeline += new QueryMonadCPSLowering(schema, IR)
    pipeline += ParameterPromotion
    pipeline += PartiallyEvaluate
    pipeline += DCE
    pipeline += TreeDumper(true)

    pipeline(IR)(block)
  }

  "cps lowering filter leftHashSemiJoin" should "work" in {
    val exp = {
      IR.reifyBlock {
        val lineitemArray = {
          import IR._
          __newArray[LINEITEMRecord](unit(100000))
        }

        dsl"""
        val l1 = Query($lineitemArray).filter(_.L_ORDERKEY == 1)
        val l2 = Query($lineitemArray)
        val l3 = l1.leftHashSemiJoin(l2)(_.L_ORDERKEY)(_.L_ORDERKEY)(_.L_ORDERKEY == _.L_ORDERKEY)
        l3.count
        """
      }
    }
    val newExp = transform(exp)
    // println(newExp)
    // TODO
  }
}
