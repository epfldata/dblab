package ch.epfl.data
package legobase
package optimization

import pardis.ir._
import pardis.types._
import pardis.types.PardisTypeImplicits._
import pardis.optimization._
import legobase.deep._

object StringCompressionTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new StringCompressionTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

class StringCompressionTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  sealed trait Phase
  case object LoadingPhase extends Phase
  case object QueryExecutionPhase extends Phase
  var phase: Phase = LoadingPhase

  val debugEnabled = false
  val compressedStringsMaps = scala.collection.mutable.Map[String, (Var[Int], Expression[ArrayBuffer[OptimalString]])]()
  val hoistedStatements = collection.mutable.Set[String]() // CAN DIE & WILL DIE SOON
  val modifiedExpressions = collection.mutable.Map[Expression[Any], String]()

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    System.out.println("StringCompressionTransformer: Modified symbols are: " + modifiedExpressions.mkString(","))
    phase = LoadingPhase
    reifyBlock {
      // Generate and hoist needed maps for string compression
      hoistedStatements.foreach(hs => {
        System.out.println("StringCompressionTransformer: Creating map for field " + hs)
        compressedStringsMaps.getOrElseUpdate(hs, {
          (__newVar[Int](unit(0)), __newArrayBuffer[OptimalString]())
        })
      })
      // Now generate rest of program
      toAtom(transformProgram(node))
    }
  }

  analysis += rule {
    case GenericEngineRunQueryObject(b) => {
      phase = QueryExecutionPhase
      traverseBlock(b)
    }
    case s @ Struct(tag, elems, methods) if phase == LoadingPhase =>
      elems.foreach(e => {
        if (e.init.tp == OptimalStringType) hoistedStatements += e.name
      })
  }

  // !!! HIGHLY EXPERIMENTAL DEPENDENCY ANALYSIS (is there a generic mechanism for this is SysC?)
  def checkAndAddDependency(c: Expression[Any], a: Expression[Any]*): Unit = {
    if (modifiedExpressions.contains(c)) a.foreach(e => {
      modifiedExpressions += e -> modifiedExpressions(c)
    })
  }
  analysis += statement {
    case sym -> Struct(tag, elems, methods) if phase == QueryExecutionPhase =>
      elems.foreach(e => {
        if (e.init.tp == OptimalStringType) modifiedExpressions += sym -> {
          e.init match {
            case Def(PardisStructImmutableField(s, f)) => f
          }
        }
      })
    case sym -> PardisStructImmutableField(s, f) if phase == QueryExecutionPhase =>
      checkAndAddDependency(s, sym)
    case sym -> HashMapGetOrElseUpdate(map, key, Block(b, res)) =>
      checkAndAddDependency(res, sym, map)
    case sym -> HashMapForeach(map, Def(PardisLambda(f, i, b))) =>
      checkAndAddDependency(map, i)
      traverseBlock(b)
    case sym -> TreeSet$plus$eq(tree, elem) =>
      checkAndAddDependency(elem, tree)
      tree match {
        case Def(TreeSetNew2(Def(OrderingNew(Def(PardisLambda2(f, i1, i2, b)))))) =>
          checkAndAddDependency(elem, i1)
          checkAndAddDependency(elem, i2)
          traverseBlock(b)
        case _ => throw new Exception("StringCompressionTransformer BUG: unknown node type in analysis of trees: " + tree.correspondingNode)
      }
    case sym -> TreeSetHead(tree)      => checkAndAddDependency(tree, sym)
    case sym -> Tuple2_Field__1(tuple) => checkAndAddDependency(tuple, sym)
    case sym -> Tuple2_Field__2(tuple) => checkAndAddDependency(tuple, sym)
  }

  rewrite += rule {
    case origStruct @ Struct(tag, elems, methods) if phase == LoadingPhase =>
      if (debugEnabled) System.out.println("StringCompressionTransformer: Struct " + tag + " found with elems " + elems.mkString)

      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType) {
          if (debugEnabled) System.out.println("StringCompressionTransformer: Field " + e.name + " of type " + e.init.tp)
          if (debugEnabled) System.out.println("StringCompressionTransformer: Map now is " + compressedStringsMaps.mkString)

          val compressedStringMetaData = compressedStringsMaps(e.name)
          val num_unique_strings = compressedStringMetaData._1
          val compressedStringValues = compressedStringMetaData._2

          val uncompressedString = e.init.asInstanceOf[Expression[OptimalString]]
          __ifThenElse(!compressedStringValues.contains(uncompressedString), {
            __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
            compressedStringValues.append(uncompressedString)
          }, unit)
          PardisStructArg(e.name, e.mutable, compressedStringValues.indexOf(uncompressedString))
        } else e

      }), methods)(origStruct.tp)

    case origStruct @ Struct(tag, elems, methods) if phase == QueryExecutionPhase =>
      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType) {
          PardisStructArg(e.name, e.mutable, infix_asInstanceOf(e.init)(typeInt))
        } else e
      }), methods)(origStruct.tp)
  }

  // Overwrite behaviour of string operations
  rewrite += rule {
    case OptimalString$eq$eq$eq(str1, str2) => str2 match {
      case Def(GenericEngineParseStringObject(constantString)) =>
        if (debugEnabled) System.out.println("StringCompressionTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            val compressedStringMetaData = compressedStringsMaps(name)
            val compressedStringValues = compressedStringMetaData._2
            val compressedConstantString = compressedStringValues.indexOf(str2)
            infix_==(str1, compressedConstantString)
          case dflt @ _ =>
            throw new Exception("StringCompressionTransformer BUG: unknown node type in LHS of comparison with constant string. LHS node is " + str1.correspondingNode)
        }
      case _ => infix_==(str1, str2)
    }

    // This usually appears in sorting
    case OptimalStringDiff(str1, str2) =>
      val fieldName = modifiedExpressions.get(str1).get
      val compressedStringValues = compressedStringsMaps(fieldName)._2
      val uncompressedStr1 = compressedStringValues(str1.asInstanceOf[Expression[Int]])
      val uncompressedStr2 = compressedStringValues(str2.asInstanceOf[Expression[Int]])
      optimalStringDiff(uncompressedStr1, uncompressedStr2)

    case OptimalStringString(str) => {
      System.out.println("StringCompressionTransformer: Stringification of compressed string: Corresponding node is " + str.correspondingNode)
      modifiedExpressions.get(str) match {
        case Some(fieldName) =>
          val compressedStringMetaData = compressedStringsMaps(fieldName)
          val compressedStringValues = compressedStringMetaData._2
          compressedStringValues(str.asInstanceOf[Expression[Int]])
        case None =>
          throw new Exception("StringCompressionTransformer BUG: unknown node type in stringification of compressed string. LHS node is " + str.correspondingNode)
      }
    }
  }

  rewrite += rule {
    case hm @ HashMapNew() if hm.typeA == OptimalStringType =>
      System.out.println("StringCompressionTransformer: Replacing map of OptimalString with map of Int")
      HashMapNew()(typeRep[Int].asInstanceOf[PardisType[Any]], hm.typeB)
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) => {
      phase = QueryExecutionPhase
      val tb = transformBlock(b)
      GenericEngineRunQueryObject(tb)
    }
  }
}
