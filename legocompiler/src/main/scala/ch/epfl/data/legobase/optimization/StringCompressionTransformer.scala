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
  // Metadata needed for twoPhaseStringCompression
  var twoPhaseStringCompressionNeeded: Boolean = false
  var stringReversalNeeded: Boolean = false // Used for endsWith
  val scanOperatorArrays = new scala.collection.mutable.ArrayBuffer[(Expression[Array[Any]], Seq[String])]()

  val debugEnabled = false
  // The third argument of the following map is used only for the twoPhaseStringCompression
  var compressedStringsMaps = scala.collection.mutable.Map[String, (Var[Int], Expression[ArrayBuffer[OptimalString]], Expression[ArrayBuffer[OptimalString]])]()
  val hoistedStatements = collection.mutable.Set[String]()
  val modifiedExpressions = collection.mutable.Map[Expression[Any], String]()
  case class ConstantStringInfo(val poolName: String, val isStartsWithOperation: Boolean)
  val constantStrings = collection.mutable.Map[Expression[Any], ConstantStringInfo]()
  val nameAliases = collection.mutable.Map[String, String]()

  def getNameAliasIfAny(str: String) = nameAliases.getOrElse(str, {
    if (debugEnabled) System.out.println("StringCompressionTransformer: Name alias for string " + str + " not found!")
    str
  })

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    //System.out.println("StringCompressionTransformer: Modified symbols are:\n" + modifiedExpressions.mkString("\n"))
    phase = LoadingPhase
    reifyBlock {
      // Generate and hoist needed maps for string compression
      hoistedStatements.foreach(hs => {
        System.out.println("StringCompressionTransformer: Creating map for field " + hs)
        compressedStringsMaps.getOrElseUpdate(hs, {
          (__newVar[Int](unit(0)), __newArrayBuffer[OptimalString](), {
            if (twoPhaseStringCompressionNeeded) __newArrayBuffer[OptimalString]() else unit(null)
          })
        })
      })
      // Now generate rest of program
      toAtom(transformProgram(node))
    }
  }

  def array_foreach[T: TypeRep](arr: Rep[Array[T]], f: Rep[T] => Rep[Unit]): Rep[Unit] = {
    Range(unit(0), arr.length).foreach {
      __lambda { i =>
        val e = arr(i)
        f(e)
      }
    }
  }

  analysis += rule {
    case ArrayUpdate(array, idx, Def(Struct(tag, elems, methods))) if phase == LoadingPhase => {
      scanOperatorArrays += new Tuple2(array.asInstanceOf[Expression[Array[Any]]],
        elems.filter(e => e.init.tp == OptimalStringType).map(e => e.name))
      ()
    }
  }

  // Used to hoist out compression of constant strings
  analysis += rule {
    case OptimalStringComparison(Def(PardisStructImmutableField(s, name)),
      str2 @ Def(GenericEngineParseStringObject(constantString)), _) =>
      constantStrings += str2 -> ConstantStringInfo(name, false)
      ()
    case OptimalStringStartsOrEndsWith(Def(PardisStructImmutableField(s, name)),
      str2 @ Def(GenericEngineParseStringObject(constantString)), reverseString) =>
      constantStrings += str2 -> ConstantStringInfo(name, true)
      twoPhaseStringCompressionNeeded = true
      if (reverseString) stringReversalNeeded = true
      ()
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
            case Def(PardisStructImmutableField(s, f)) => {
              if (e.name != f) {
                System.out.println("StringCompressionTransformer: Registering name alias: " + e.name + " -> " + getNameAliasIfAny(f))
                nameAliases += e.name -> getNameAliasIfAny(f)
              }
              f
            }
            case dflt @ _ => throw new Exception("StringCompressionTransformer BUG: unknown node type in analysis of structs: " + dflt.correspondingNode)
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

  // Transformers
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
          // Compress string (if twoPhaseStringCompressionNeeded then the value assigned is wrong and we fix it later)
          val uncompressedString = {
            if (stringReversalNeeded) e.init.asInstanceOf[Expression[OptimalString]].reverse
            else e.init.asInstanceOf[Expression[OptimalString]]
          }
          if (twoPhaseStringCompressionNeeded)
            compressedStringMetaData._3.append(uncompressedString)
          __ifThenElse(!compressedStringValues.contains(uncompressedString), {
            __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
            compressedStringValues.append(uncompressedString)
          }, unit)
          PardisStructArg(e.name, true, compressedStringValues.indexOf(uncompressedString))
        } else e

      }), methods)(origStruct.tp)

    case origStruct @ Struct(tag, elems, methods) if phase == QueryExecutionPhase =>
      System.out.println("StringCompressionTransformer: Rewriting struct during query with tag: " + tag)
      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType) {
          PardisStructArg(e.name, e.mutable, infix_asInstanceOf(apply(e.init))(typeInt))
        } else e
      }), methods)(origStruct.tp)
  }

  rewrite += rule {
    case sf @ StructImmutableField(s, f) if sf.tp == OptimalStringType => field[Int](apply(s), f)
    case sf @ StructFieldGetter(s, f) if sf.tp == OptimalStringType    => fieldGetter[Int](apply(s), f)
  }

  object OptimalStringComparison {
    def unapply(node: Def[Any]): Option[(Rep[OptimalString], Rep[OptimalString], Boolean)] = node match {
      case OptimalString$eq$eq$eq(str1, str2)   => Some((str1, str2, true))
      case OptimalString$eq$bang$eq(str1, str2) => Some((str1, str2, false))
      case _                                    => None
    }
  }

  object OptimalStringStartsOrEndsWith {
    def unapply(node: Def[Any]): Option[(Rep[OptimalString], Rep[OptimalString], Boolean)] = node match {
      case OptimalStringStartsWith(str1, str2) => Some((str1, str2, false))
      case OptimalStringEndsWith(str1, str2)   => Some((str1, str2, true))
      case _                                   => None
    }
  }

  // Overwrite behaviour of string operations
  rewrite += statement {
    case sym -> GenericEngineParseStringObject(constantString) =>
      val csi = constantStrings(sym)
      System.out.println("StringCompressionTransformer: Generating code for compressing constant string " + csi.poolName)
      val newSym = GenericEngine.parseString(constantString)
      val str2tmp = if (stringReversalNeeded) newSym.reverse else newSym
      val compressedStringMetaData = compressedStringsMaps(getNameAliasIfAny(csi.poolName))
      val compressedStringValues = compressedStringMetaData._2
      if (!csi.isStartsWithOperation) compressedStringValues.indexOf(str2tmp)
      else {
        val startIndex = compressedStringValues.indexWhere({ (str: Rep[OptimalString]) => str.startsWith(str2tmp) })
        val endIndex = compressedStringValues.lastIndexWhere({ (str: Rep[OptimalString]) => str.startsWith(str2tmp) })
        Tuple2(startIndex, endIndex)
      }
  }

  rewrite += rule {
    case OptimalStringComparison(str1, str2, equalityCheck) => str2 match {
      case Def(GenericEngineParseStringObject(constantString)) =>
        if (debugEnabled) System.out.println("StringCompressionTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            if (equalityCheck) infix_==(apply(str1), apply(str2))
            else infix_!=(apply(str1), apply(str2))
          case dflt @ _ =>
            throw new Exception("StringCompressionTransformer BUG: unknown node type in LHS of comparison with constant string. LHS node is " + str1.correspondingNode)
        }
      case _ => if (equalityCheck) infix_==(str1, str2) else infix_!=(str1, str2)
    }

    // This usually appears in sorting
    case OptimalStringDiff(str1, str2) =>
      modifiedExpressions.get(str1) match {
        case Some(fieldName) =>
          val compressedStringValues = compressedStringsMaps(fieldName)._2
          val uncompressedStr1 = compressedStringValues(apply(str1).asInstanceOf[Expression[Int]])
          val uncompressedStr2 = compressedStringValues(apply(str2).asInstanceOf[Expression[Int]])
          optimalStringDiff(uncompressedStr1, uncompressedStr2)
        case None =>
          // str1 && str2 are already integers by propagation of compressed string 
          // (e.g. intermediate struct allocated during query execution)
          apply(str1).asInstanceOf[Expression[Int]] - apply(str2).asInstanceOf[Expression[Int]]
      }

    case OptimalStringString(str) => {
      System.out.println("StringCompressionTransformer: Stringification of compressed string: Corresponding node is " + str.correspondingNode)
      val fieldName = modifiedExpressions.get(str) match {
        case Some(f) => getNameAliasIfAny(f)
        case None => str match {
          case Def(PardisStructImmutableField(s, f)) => getNameAliasIfAny(f)
          case _                                     => throw new Exception("StringCompressionTransformer BUG: unknown node type in stringification of compressed string. LHS node is " + str.correspondingNode)
        }
      }

      val compressedStringMetaData = compressedStringsMaps(fieldName)
      val compressedStringValues = compressedStringMetaData._2
      val tmpString = compressedStringValues(apply(str).asInstanceOf[Expression[Int]])
      if (stringReversalNeeded) tmpString.reverse else tmpString
    }

    case OptimalStringStartsOrEndsWith(str1, str2, _) => str2 match {
      case Def(GenericEngineParseStringObject(constantString)) =>
        if (debugEnabled) System.out.println("StringCompressionTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            val range = apply(str2).asInstanceOf[Expression[Tuple2[Int, Int]]]
            val index = apply(str1).asInstanceOf[Expression[Int]]
            index >= range._1 && index <= range._2
          case dflt @ _ =>
            throw new Exception("StringCompressionTransformer BUG: unknown node type in LHS of startsWith node. LHS node is " + str1.correspondingNode)
        }
      case _ => throw new Exception("StringCompressionTransformer BUG: OptimalStringStartsWith with RHS a node != GenericEngineParseStringObject is not supported yet. (RHS node is " + str2.correspondingNode + ")")
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
      System.out.println("StringCompressionTransformer: twoPhaseStringCompressionNeeded = " + twoPhaseStringCompressionNeeded)

      if (twoPhaseStringCompressionNeeded) {
        reifyBlock {
          compressedStringsMaps = compressedStringsMaps.map(csm => {
            System.out.println("StringCompressionTransformer: Emitting code for sorting string pool of field " + csm._1)
            val ab = csm._2._2
            val abSorted = ab.sortWith(doLambda2((a: Rep[OptimalString], b: Rep[OptimalString]) => optimalStringDiff(a, b) < 0))
            (csm._1, (csm._2._1, abSorted, csm._2._3))
          })
          // Now we need to update the strings with correct compressed values
          scanOperatorArrays.foreach(soa => {
            // we know it is a struct --> TODO: Update so that this works in all cases
            val arr = soa._1
            val fieldNames = soa._2
            val idx = __newVar[Int](0)
            array_foreach(arr, ((ae: Rep[Any]) => {
              fieldNames.foreach(fn => {
                val compressedStringMetaData = compressedStringsMaps(fn)
                val uncompressedString = compressedStringMetaData._3.apply(idx)
                val compressedString = compressedStringMetaData._2.indexOf(uncompressedString)
                fieldSetter(ae, fn, compressedString)
              })
              __assign(idx, readVar(idx) + 1)
            }))(arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
          })
          // Print generic block of query execution
          GenericEngineRunQueryObject(transformBlock(b))
        }
      } else GenericEngineRunQueryObject(transformBlock(b))
    }
  }
}