

package ch.epfl.data
package dblab.legobase
package optimization

import schema._
import sc.pardis.ir._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.optimization._
import dblab.legobase.deep._

/**
 * Creates a dictionary for strings in the loader and in the query processor transforms string operations to integer
 * operations.
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class StringDictionaryTransformer(override val IR: LoweringLegoBase, val schema: Schema) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._

  sealed trait Phase
  case object LoadingPhase extends Phase
  case object QueryExecutionPhase extends Phase
  var phase: Phase = LoadingPhase
  // Metadata needed for different compression algorithms
  var twoPhaseStringCompressionNeeded: Boolean = false
  var wordTokinizingStringCompressionNeeded: Boolean = false
  val tokenizedStrings = collection.mutable.Set[String]()
  var stringReversalNeeded: Boolean = false // Used for endsWith
  val scanOperatorArrays = new scala.collection.mutable.ArrayBuffer[(Expression[Array[Any]], Seq[String])]()
  val COMPRESSION_THREASHOLD = unit(4096) // maximum number of unique strings per list possible

  val debugEnabled = false
  // The third argument of the following map is used only for the twoPhaseStringCompression
  var compressedStringsMaps = scala.collection.mutable.Map[String, (Var[Int], Expression[ArrayBuffer[OptimalString]], Expression[ArrayBuffer[OptimalString]])]()
  val hoistedStatements = collection.mutable.Set[String]()
  val modifiedExpressions = collection.mutable.Map[Expression[Any], String]()
  case class ConstantStringInfo(val poolName: String, val isStartsWithOperation: Boolean)
  val constantStrings = collection.mutable.Map[Expression[Any], ConstantStringInfo]()
  val nameAliases = collection.mutable.Map[String, String]()
  val MAX_NUM_WORDS = 15;
  val max_num_words_map = scala.collection.mutable.Map[String, (Var[Int], Expression[Array[Int]])]()

  def shouldTokenize(name: String): Boolean = wordTokinizingStringCompressionNeeded && tokenizedStrings.contains(name)
  def compressedStringType(name: String): TypeRep[Any] = (if (shouldTokenize(name)) ArrayType(IntType) else IntType).asInstanceOf[TypeRep[Any]]

  def getNameAliasIfAny(str: String) = nameAliases.getOrElse(str, {
    if (debugEnabled) System.out.println("StringDictionaryTransformer: Name alias for string " + str + " not found!")
    str
  })

  // cc stands for cancompress
  def cc(name: String): Boolean = schema.findAttribute(getNameAliasIfAny(name)) match {
    case Some(attr) => attr.hasConstraint(Compressed)
    case None       => false
  }

  override def optimize[T: TypeRep](node: Block[T]): to.Block[T] = {
    traverseBlock(node)
    //System.out.println("StringDictionaryTransformer: Modified symbols are:\n" + modifiedExpressions.mkString("\n"))
    phase = LoadingPhase
    reifyBlock {
      // Generate and hoist needed maps for string compression
      hoistedStatements.foreach(hs => {
        System.out.println("StringDictionaryTransformer: Creating map for field " + hs)
        compressedStringsMaps.getOrElseUpdate(hs, {
          (__newVar[Int](unit(0)), __newArrayBuffer[OptimalString](), {
            if (twoPhaseStringCompressionNeeded) __newArrayBuffer[OptimalString]() else unit(null)
          })
        })
      })
      // Generate maps needed for tokenizing
      tokenizedStrings.foreach(ts => {
        max_num_words_map.getOrElseUpdate(ts, {
          (__newVar[Int](unit(0)), __newArray[Int](12000000)) // todo: fix with proper value
        })
      })
      // Now generate rest of program
      toAtom(transformProgram(node))
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
    case OptimalStringDiff(_, _) =>
      twoPhaseStringCompressionNeeded = true
      ()
    case OptimalStringStartsOrEndsWith(Def(PardisStructImmutableField(s, name)),
      str2 @ Def(GenericEngineParseStringObject(constantString)), reverseString) =>
      constantStrings += str2 -> ConstantStringInfo(name, true)
      twoPhaseStringCompressionNeeded = true
      if (reverseString) stringReversalNeeded = true
      ()
    case OptimalStringIndexOfSlice(Def(PardisStructImmutableField(s, name)),
      str2 @ Def(GenericEngineParseStringObject(constantString)), idx) =>
      constantStrings += str2 -> ConstantStringInfo(name, false)
      wordTokinizingStringCompressionNeeded = true
      tokenizedStrings += name
      ()
    case OptimalStringContainsSlice(Def(PardisStructImmutableField(s, name)),
      str2 @ Def(GenericEngineParseStringObject(constantString))) =>
      constantStrings += str2 -> ConstantStringInfo(name, false)
      wordTokinizingStringCompressionNeeded = true
      tokenizedStrings += name
      ()
  }

  analysis += rule {
    case GenericEngineRunQueryObject(b) => {
      phase = QueryExecutionPhase
      traverseBlock(b)
    }
    case s @ Struct(tag, elems, methods) if phase == LoadingPhase =>
      elems.foreach(e => {
        if (e.init.tp == OptimalStringType && cc(e.name)) hoistedStatements += e.name
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
        if (e.init.tp == OptimalStringType) {
          e.init match {
            case Def(PardisStructImmutableField(s, f)) => {
              if (e.name != f) {
                System.out.println("StringDictionaryTransformer: Registering name alias: " + e.name + " -> " + getNameAliasIfAny(f))
                nameAliases += e.name -> getNameAliasIfAny(f)
              }
              modifiedExpressions += sym -> f
            }
            case _ =>
            //case dflt @ _ => throw new Exception("StringDictionaryTransformer BUG: unknown node type in analysis of structs: " + dflt /* + dflt.correspondingNode*/ )
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
        case Def(PardisStructImmutableField(a, b)) => // TODO: Check what is needed to be done here -- for now it is OK
        case _                                     => throw new Exception("StringDictionaryTransformer BUG: unknown node type in analysis of trees: " + tree.correspondingNode)
      }
    case sym -> TreeSetHead(tree)      => checkAndAddDependency(tree, sym)
    case sym -> Tuple2_Field__1(tuple) => checkAndAddDependency(tuple, sym)
    case sym -> Tuple2_Field__2(tuple) => checkAndAddDependency(tuple, sym)
  }

  // Transformers
  rewrite += rule {
    case origStruct @ Struct(tag, elems, methods) if phase == LoadingPhase =>
      if (debugEnabled) System.out.println("StringDictionaryTransformer: Struct " + tag + " found with elems " + elems.mkString)

      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType && cc(e.name)) {
          //System.out.println(e.name)
          if (debugEnabled) System.out.println("StringDictionaryTransformer: Field " + e.name + " of type " + e.init.tp)
          if (debugEnabled) System.out.println("StringDictionaryTransformer: Map now is " + compressedStringsMaps.mkString)

          val compressedStringMetaData = compressedStringsMaps(e.name)
          val num_unique_strings = compressedStringMetaData._1
          val compressedStringValues = compressedStringMetaData._2

          val uncompressedString = {
            if (stringReversalNeeded) e.init.asInstanceOf[Expression[OptimalString]].reverse
            else e.init.asInstanceOf[Expression[OptimalString]]
          }

          // Compress string according to algorithm (if twoPhaseStringCompressionNeeded then the value assigned 
          // here is wrong and we fix it later)
          if (twoPhaseStringCompressionNeeded)
            compressedStringMetaData._3.append(uncompressedString)

          // Emit check for large lists -- this is to avoid having compression on strings that have many unique values
          __ifThenElse(readVar(num_unique_strings) > COMPRESSION_THREASHOLD, {
            printf(unit("StringDictionaryTransformer ERROR: Number of unique strings in list of field %s exceeded the maximum allowed value of %d\n"), unit(e.name), COMPRESSION_THREASHOLD)
            printf(unit("\t\tEither disable StringDictionaryTransformer for your query, or disable %s in the transformer.\n"), unit(e.name))
            // TODO: Lift System.exit so that we can exit, or throw exception in the generated code
          }, unit())

          if (shouldTokenize(e.name)) {
            val tokenizedStringInfo = max_num_words_map(e.name)
            System.out.println("StringDictionaryTransformer: tokenizing " + e.name);
            val delim = __newArray[Byte](8)
            delim(0) = unit(' '); delim(1) = unit('.'); delim(2) = unit('!'); delim(3) = unit(';');
            delim(4) = unit(','); delim(5) = unit(':'); delim(6) = unit('?'); delim(7) = unit('-');

            val compressedWords = __newArray[Int](MAX_NUM_WORDS)
            Range(0, MAX_NUM_WORDS).foreach { __lambda { i => compressedWords(i) = unit(-1) } }
            val words = uncompressedString.split(delim.asInstanceOf[Rep[Array[Char]]])
            val stringIdx =
              Range(0, MAX_NUM_WORDS).foreach {
                __lambda { i =>
                  val w = words(i)
                  __ifThenElse(w __!= unit(null), {
                    __ifThenElse(!compressedStringValues.contains(w), {
                      __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
                      compressedStringValues.append(w)
                    }, unit)
                    compressedWords(i) = compressedStringValues.indexOf(w)
                    arrayUpdate(tokenizedStringInfo._2, tokenizedStringInfo._1, tokenizedStringInfo._2(tokenizedStringInfo._1) + unit(1))
                  }, unit())
                }
              }
            //printf(unit("%d\n"), tokenizedStringInfo._2(tokenizedStringInfo._1))
            __assign(tokenizedStringInfo._1, readVar(tokenizedStringInfo._1) + unit(1))

            /*val compressedWords = words.map((w: Expression[OptimalString]) => {
              __ifThenElse(!compressedStringValues.contains(w), {
                __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
                compressedStringValues.append(w)
              }, unit)
              compressedStringValues.indexOf(w)
            })*/
            PardisStructArg(e.name, true, compressedWords)
          } else {
            __ifThenElse(!compressedStringValues.contains(uncompressedString), {
              __assign(num_unique_strings, readVar(num_unique_strings) + unit(1))
              compressedStringValues.append(uncompressedString)
            }, unit)
            PardisStructArg(e.name, true, compressedStringValues.indexOf(uncompressedString))
          }
        } else e

      }), methods)(origStruct.tp)

    case origStruct @ Struct(tag, elems, methods) if phase == QueryExecutionPhase =>
      Struct(tag, elems.map(e => {
        if (e.init.tp == OptimalStringType && cc(e.name)) {
          System.out.println("StringDictionaryTransformer: Rewriting struct during query with tag: " + tag + " / field: " + e.name)
          PardisStructArg(e.name, e.mutable, infix_asInstanceOf(apply(e.init))(compressedStringType(e.name)))
        } else e
      }), methods)(origStruct.tp)
  }

  rewrite += rule {
    case sf @ StructImmutableField(s, f) if sf.tp == OptimalStringType && cc(f) => field(apply(s), f)(compressedStringType(f))
    case sf @ StructFieldGetter(s, f) if sf.tp == OptimalStringType && cc(f)    => fieldGetter(apply(s), f)(compressedStringType(f))
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
    case sym -> GenericEngineParseStringObject(constantString) if constantStrings.contains(sym) && cc(constantStrings(sym).poolName) =>
      val csi = constantStrings(sym)
      System.out.println("StringDictionaryTransformer: Generating code for compressing constant string " + csi.poolName)
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
        if (debugEnabled) System.out.println("StringDictionaryTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            if (cc(name)) {
              if (equalityCheck) infix_==(apply(str1), apply(str2))
              else infix_!=(apply(str1), apply(str2))
            } else {
              if (equalityCheck) optimalString$eq$eq$eq(str1, str2)
              else optimalString$eq$bang$eq(str1, str2)
            }
          case dflt @ _ =>
            throw new Exception("StringDictionaryTransformer BUG: unknown node type in LHS of comparison with constant string. LHS node is " + str1.correspondingNode)
        }
      case _ => if (equalityCheck) infix_==(str1, str2) else infix_!=(str1, str2)
    }

    // This usually appears in sorting
    case OptimalStringDiff(str1, str2) =>
      modifiedExpressions.get(str1) match {
        case Some(fieldName) =>
          if (cc(fieldName)) {
            val compressedStringValues = compressedStringsMaps(fieldName)._2
            val uncompressedStr1 = compressedStringValues(apply(str1).asInstanceOf[Expression[Int]])
            val uncompressedStr2 = compressedStringValues(apply(str2).asInstanceOf[Expression[Int]])
            optimalStringDiff(uncompressedStr1, uncompressedStr2)
          } else optimalStringDiff(str1, str2)
        case None =>
          str1 match {
            case Def(PardisStructImmutableField(s, f)) => if (cc(f)) {
              // str1 && str2 are already integers by propagation of compressed string 
              // (e.g. intermediate struct allocated during query execution)
              apply(str1).asInstanceOf[Expression[Int]] - apply(str2).asInstanceOf[Expression[Int]]
            } else optimalStringDiff(str1, str2)
            case _ => throw new Exception("StringDictionaryTransformer BUG: OptimalStringDiff " + str1.correspondingNode)
          }
      }

    case OptimalStringString(str) => {
      val fieldName = modifiedExpressions.get(str) match {
        case Some(f) => getNameAliasIfAny(f)
        case None => str match {
          case Def(PardisStructImmutableField(s, f)) => getNameAliasIfAny(f)
          case _                                     => throw new Exception("StringDictionaryTransformer BUG: unknown node type in stringification of compressed string. LHS node is " + str.correspondingNode)
        }
      }
      if (cc(fieldName)) {
        System.out.println("StringDictionaryTransformer: Stringification of compressed string: Corresponding node is " + str.correspondingNode)
        val compressedStringMetaData = compressedStringsMaps(fieldName)
        val compressedStringValues = compressedStringMetaData._2
        val tmpString = compressedStringValues(apply(str).asInstanceOf[Expression[Int]])
        if (stringReversalNeeded) tmpString.reverse else tmpString
      } else optimalStringString(str)
    }

    case OptimalStringStartsOrEndsWith(str1, str2, endsWith) => str2 match {
      case Def(GenericEngineParseStringObject(constantString)) =>
        if (debugEnabled) System.out.println("StringDictionaryTransformer: GenericEngineParseStringObject of " + constantString + " encountered ")
        str1 match {
          case Def(PardisStructImmutableField(s, name)) =>
            if (cc(name)) {
              val range = apply(str2).asInstanceOf[Expression[Tuple2[Int, Int]]]
              val index = apply(str1).asInstanceOf[Expression[Int]]
              index >= range._1 && index <= range._2
            } else {
              if (endsWith) optimalStringEndsWith(str1, str2)
              else optimalStringStartsWith(str1, str2)
            }
          case dflt @ _ =>
            throw new Exception("StringDictionaryTransformer BUG: unknown node type in LHS of startsWith node. LHS node is " + str1.correspondingNode)
        }
      case _ => throw new Exception("StringDictionaryTransformer BUG: OptimalStringStartsWith with RHS a node != GenericEngineParseStringObject is not supported yet. (RHS node is " + str2.correspondingNode + ")")
    }
  }

  rewrite += rule {
    case hm @ HashMapNew() if hm.typeA == OptimalStringType && modifiedExpressions.size != 0 =>
      System.out.println("StringDictionaryTransformer: Replacing map of OptimalString with map of Int")
      HashMapNew()(typeRep[Int].asInstanceOf[PardisType[Any]], hm.typeB)
  }

  rewrite += rule {
    case GenericEngineRunQueryObject(b) => {
      phase = QueryExecutionPhase
      System.out.println("StringDictionaryTransformer: twoPhaseStringCompressionNeeded = " + twoPhaseStringCompressionNeeded)
      System.out.println("StringDictionaryTransformer: wordTokinizingStringCompressionNeeded = " + wordTokinizingStringCompressionNeeded)
      System.out.println("StringDictionaryTransformer: tokenized strings: " + tokenizedStrings.mkString(","))
      max_num_words_map.foreach(ts => {
        //printf(unit("%d"), readVar(ts._2._1)(IntType))
        __assign(ts._2._1, unit(-1))
      })
      if (twoPhaseStringCompressionNeeded) {
        reifyBlock {
          compressedStringsMaps = compressedStringsMaps.map(csm => {
            System.out.println("StringDictionaryTransformer: Emitting code for sorting string pool of field " + csm._1)
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
              fieldNames.filter(fn => !shouldTokenize(fn) && cc(fn)).foreach(fn => {
                val compressedStringMetaData = compressedStringsMaps(fn)
                val uncompressedString = compressedStringMetaData._3.apply(idx)
                val compressedString = compressedStringMetaData._2.indexOf(uncompressedString)
                fieldSetter(ae, fn, compressedString)
              })
              __assign(idx, readVar(idx) + 1)
            }))(arr.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
          })
          // Print generic block of query execution
          GenericEngineRunQueryObject(transformBlock(b)(b.tp))(b.tp)
        }
      } else GenericEngineRunQueryObject(transformBlock(b)(b.tp))(b.tp)
    }
  }

  rewrite += rule {
    case NotEqual(elem, value) => {
      elem match {
        case Def(OptimalStringIndexOfSlice(str1, str2, beg)) => str1 match {
          case Def(PardisStructImmutableField(s, name)) if (cc(name)) => NotEqual(apply(elem), unit(MAX_NUM_WORDS))
          case _ => NotEqual(apply(elem), apply(value))
        }
        case _ => NotEqual(apply(elem), apply(value))
      }
    }
  }

  rewrite += rule {
    case OptimalStringIndexOfSlice(str1, str2, beg) =>
      str1 match {
        case Def(PardisStructImmutableField(s, name)) if (cc(name)) =>
          System.out.println("AM I EVER HERE FOR " + name + " ? ")
          val tokenizedStringInfo = max_num_words_map(name)
          val idx = __newVarNamed[Int](MAX_NUM_WORDS, "findSlice")
          val start = __newVar[Int](apply(beg) match {
            case Constant(v: Int) => {
              __assign(tokenizedStringInfo._1, readVar(tokenizedStringInfo._1) + unit(1))
              unit(v)
            }
            case value => value //__ifThenElse(value __== unit(-1), MAX_NUM_WORDS, value)
          })
          val num_words = tokenizedStringInfo._2(tokenizedStringInfo._1)
          //val i = __newVar[Int](__ifThenElse(apply(beg) __== unit(-1), MAX_NUM_WORDS, apply(beg)))
          //Range(apply(beg), MAX_NUM_WORDS /* apply(str1).length */ ).foreach {
          //__lambda { i =>
          __whileDo(readVar(start) < num_words /*MAX_NUM_WORDS*/ , {
            val wordI = toAtom(ArrayApply(apply(str1).asInstanceOf[Expression[Array[Int]]], readVar(start)))(IntType)
            __ifThenElse(wordI __== apply(str2), {
              // __ifThenElse(readVar(idx) __== unit(-1), __assign(idx, i), unit())
              __assign(idx, readVar(start))
              break
            }, unit())
            __assign(start, readVar(start) + unit(1))
          })
          //}
          //}
          readVar(idx)
        case Def(PardisStructImmutableField(s, name)) if (!cc(name)) => optimalStringIndexOfSlice(str1, str2, apply(beg))
        case _ => throw new Exception("StringDictionaryTransformer BUG: OptimalStringIndexOfSlice with a node != PardisStructImmutableField (node is of type " + str1.correspondingNode + ")")
      }

    case OptimalStringContainsSlice(str1, str2) =>
      str1 match {
        case Def(PardisStructImmutableField(s, name)) if (cc(name)) =>
          val idx = __newVarNamed[Int](unit(-1), "containsSlice")
          Range(unit(0), /*apply(str1).length*/ unit(15)).foreach {
            __lambda { i =>
              val wordI = toAtom(ArrayApply(apply(str1).asInstanceOf[Expression[Array[Int]]], i))(IntType)
              __ifThenElse(wordI __== apply(str2), {
                __assign(idx, i)
              }, unit())
            }
          }
          __ifThenElse(readVar(idx) __!= unit(-1), unit(true), unit(false))
        case Def(PardisStructImmutableField(s, name)) if (!cc(name)) => optimalStringContainsSlice(str1, str2)
        case _ => throw new Exception("StringDictionaryTransformer BUG: OptimalStringContainsSlice with a node != PardisStructImmutableField (node is of type " + str1.correspondingNode + ")")
      }
  }
}
