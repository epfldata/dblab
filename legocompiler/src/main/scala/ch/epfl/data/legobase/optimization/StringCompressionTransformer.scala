package ch.epfl.data
package legobase
package optimization

//import scala.collection.mutable.ArrayBuffer
import pardis.ir._
import pardis.types._
import pardis.types.PardisTypeImplicits._
//import pardis.deep.scalalib._
//import pardis.deep.scalalib.collection._
import pardis.optimization._
import legobase.deep._

object StringCompressionTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new StringCompressionTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class StringCompressionTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  //import CNodes._
  //import CTypes._

  sealed trait Phase
  case object LoadingPhase extends Phase
  case object QueryPhase extends Phase
  var phase: Phase = LoadingPhase

  val compressedStringsMaps = scala.collection.mutable.Map[String, (Var[Int], Expression[HashMap[OptimalString, Int]])]()
  val hoistedStatements = collection.mutable.ArrayBuffer[String]()

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    reifyBlock {
      hoistedStatements.foreach(hs => {
        compressedStringsMaps.getOrElseUpdate(hs, {
          (__newVar[Int](unit(0)), hashMapNew[OptimalString, Int]())
        }) // Get list of unique values for this string
      })
      // your reifying statements 
      toAtom(transformProgram(node))
    }
  }
  /*analysis += statement {
    case sym -> K2DBScannerNext1(Def(K2DBScannerNew(Constant(filename))), buf) =>
      System.out.println("StringCompressionTransformer Analysis: Relation file found: " + filename)
  }*/

  analysis += rule {
    case Struct(tag, elems, methods) => elems.foreach(e => {
      if (e.init.tp == OptimalStringType) {
        hoistedStatements += e.name
      }
    })
  }

  rewrite += rule {
    case origStruct @ Struct(tag, elems, methods) if phase == LoadingPhase =>
      System.out.println("StringCompressionTransformer: Struct " + tag + " found with elems " + elems.mkString)
      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType) {
          System.out.println("StringCompressionTransformer: Field " + e.name + " of type " + e.init.tp)

          //val uncompressedString = init

          System.out.println("StringCompressionTransformer: Map now is " + compressedStringsMaps.mkString)
          val compressedStringMetaData = compressedStringsMaps(e.name)
          val num_unique_strings = compressedStringMetaData._1
          val compressedStringValues = compressedStringMetaData._2

          val uncompressedString = e.init.asInstanceOf[Expression[OptimalString]]
          __ifThenElse(!compressedStringValues.contains(uncompressedString), {
            val counterValue = readVar(num_unique_strings)
            compressedStringValues.update(uncompressedString, counterValue)
            __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
          }, unit)
          // Update the list if needed
          //map.getOrElseUpdate()
          PardisStructArg(e.name, e.mutable, compressedStringValues(uncompressedString))
        } else e

      }), methods)(origStruct.tp)
    case origStruct @ Struct(tag, elems, methods) if phase == QueryPhase =>
      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType) {
          /*  System.out.println("StringCompressionTransformer: Field " + e.name + " of type " + e.init.tp)

          //val uncompressedString = init
          val compressedStringMetaData = compressedStringsMaps.getOrElseUpdate(e.name, {
            (__newVar[Int](unit(0)), hashMapNew[OptimalString, Int]())
          }) // Get list of unique values for this string
          System.out.println("StringCompressionTransformer: Map now is " + compressedStringsMaps.mkString)

          val num_unique_strings = compressedStringMetaData._1
          val compressedStringValues = compressedStringMetaData._2

          val uncompressedString = e.init.asInstanceOf[Expression[OptimalString]]
          __ifThenElse(!compressedStringValues.contains(uncompressedString), {
            val counterValue = readVar(num_unique_strings)
            compressedStringValues.update(uncompressedString, counterValue)
            __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
          }, unit)
          // Update the list if needed
          //map.getOrElseUpdate()
          PardisStructArg(e.name, e.mutable, readVar(num_unique_strings))*/
          PardisStructArg(e.name, e.mutable, infix_asInstanceOf(e.init)(typeInt))
        } else e

      }), methods)(origStruct.tp)
  }
  /*rewrite += rule {
    case PardisStructDef(tag, elems, methods) =>
      System.out.println("StringCompressionTransformer: StructDef " + tag + " encountered!")
      PardisStructDef(tag, elems.map(e => StructElemInformation(e.name,
        if (e.tpe == OptimalStringType) typeInt.asInstanceOf[PardisType[Any]] else e.tpe,
        e.mutable)), methods)
  }*/
  //  rewrite += remove { case GenericEngineParseStringObject(constantString) => () }

  rewrite += rule {
    case OptimalString$eq$eq$eq(str1, str2) => str2 match {
      case Def(GenericEngineParseStringObject(constantString)) =>
        System.out.println("StringCompressionTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            val compressedStringMetaData = compressedStringsMaps(name)
            val compressedStringValues = compressedStringMetaData._2
            val compressedConstantString = compressedStringValues(str2)
            infix_==(str1, compressedConstantString)
          case dflt @ _ =>
            throw new Exception("StringCompressionTransformer BUG: unknown node type in LHS of comparison with constant string. LHS node is " + str1.correspondingNode)

        }
      case _ => infix_==(str1, str2)
    }
  }
  rewrite += rule {
    case GenericEngineRunQueryObject(b) => {
      phase = QueryPhase
      val tb = transformBlock(b)
      /*System.out.println("StringCompressionTransformer: StructsDefMap is " + structsDefMap.mkString("\n"))
      structsDefMap.map(sd => (sd._1, {
        val structDef = sd._2
        val newFields = structDef.fields.map(e => StructElemInformation(e.name,
          if (e.tpe == OptimalStringType) {
            System.out.println("StringCompressionTransformer: field " + e.name + " of type " + e.tpe + " in struct " + sd._1 + " changed to INT")
            typeInt.asInstanceOf[PardisType[Any]]
          } else e.tpe,
          e.mutable))
        PardisStructDef(structDef.tag, newFields, structDef.methods)
      }))
      System.out.println("StringCompressionTransformer: StructsDefMap is " + structsDefMap.mkString("\n"))
*/
      GenericEngineRunQueryObject(tb)
    }
  }
}