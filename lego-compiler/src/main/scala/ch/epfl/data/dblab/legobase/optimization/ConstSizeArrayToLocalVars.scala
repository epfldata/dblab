package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue
import sc.pardis.quasi.anf._
import quasi._

/**
 * Factory for creating instances of [[ConstSizeArrayToLocalVars]]
 */
object ConstSizeArrayToLocalVars extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ConstSizeArrayToLocalVars(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

/**
 * Transforms the elements of an array of a constant and small size into local variables.
 *
 * As a simple example, the following code:
 * {{{
 *      val array = new Array[Int](2)
 *      array(0) = foo()
 *      array(1) = goo()
 *      ...
 *      process(array(0), array(1))
 * }}}
 * is converted to:
 * {{{
 *      val array0 = foo()
 *      val array1 = goo()
 *      ...
 *      process(array0, array1)
 * }}}
 *
 * The interesting case is when a desired array is a field of a record. Then, that array field
 * is converted to several fields in the record representing the elements of that array.
 *
 * As an example, the following code:
 * {{{
 *      val record = Record {
 *        val arr = new Array[Int](2)
 *      }
 *      record.arr(0) = foo()
 *      record.arr(1) = goo()
 *      ...
 *      process(record.arr(0), record.arr(1))
 * }}}
 * is converted to:
 * {{{
 *      val record = Record {
 *        var arr: Int = _
 *        var arr_1: Int = _
 *      }
 *      record.arr = foo()
 *      record.arr_1 = goo()
 *      ...
 *      process(record.arr, record.arr_1)
 * }}}
 *
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ConstSizeArrayToLocalVars(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR)
  with StructCollector[LoweringLegoBase] {
  import IR.{ __struct_field => _, Binding => _, _ }

  // TODO To make it always sound, another analysis is needed to check if the elements of that array 
  // are always accessed with a constant index

  /**
   * Keeps the symbols of the constant size arrays
   */
  val constSizeArrays = scala.collection.mutable.Set[Rep[Any]]()
  /**
   * Keeps the size of constant size array symbols
   */
  val constSizeArraysSize = scala.collection.mutable.Map[Rep[Any], Int]()

  /**
   * Keeps the size of constant size arrays that are fields of a struct
   */
  val structsConstSizeArrayFieldSize = scala.collection.mutable.Map[(TypeRep[Any], String), Int]()

  def isConstSizeArray[T](a: Rep[T]): Boolean =
    constSizeArrays.contains(a.asInstanceOf[Rep[Any]])

  def typeMayBeConstSizeArray[T](t: TypeRep[T]): Boolean =
    constSizeArrays.exists(x => x.tp == t.asInstanceOf[TypeRep[Any]])

  def structHasSingletonArrayAsField(s: Struct[Any]) =
    s.elems.find(e => isConstSizeArray(e.init) && e.init.tp.isArray).nonEmpty

  def structSymHasSingletonArrayAsField[T](s: Rep[T]) = {
    getStructDef(s.tp) match {
      case Some(sd) => sd.fields.find(e => e.tpe.isArray && typeMayBeConstSizeArray(e.tpe)).nonEmpty
      case None     => false
    }
  }

  /**
   * The maximum threshould which is used for identifying the constant size
   * arrays that should be converted to local variables.
   */
  val SIZE_THRESHOLD = 10

  /**
   * Identifies the array symbols that should be converted to local variables.
   */
  analysis += statement {
    case sym -> dsl"new Array[Any](${ Constant(v) })" =>
      if (v < SIZE_THRESHOLD) {
        constSizeArraysSize(sym.asInstanceOf[Rep[Any]]) = v
        constSizeArrays += sym
        ()
      }
  }

  /**
   * Identifies the record field symbols that are constant size arrays.
   */
  analysis += statement {
    case sym -> dsl"__struct_field($s, $f)" if sym.tp.isArray && structSymHasSingletonArrayAsField(s) =>
      constSizeArrays += sym
      ()
  }

  /**
   * Returns the default value for the given type.
   * For the primitive types returns their corresponding default value (e.g. for
   * Int it returns 0) and for other types it returns null.
   */
  def getDefaultValue[T](tp: PardisType[T]): Rep[Any] = {
    val dfltV = sc.pardis.shallow.utils.DefaultValue(tp.name)
    if (dfltV != null) unit(0)(tp.asInstanceOf[PardisType[Int]])
    else unit(dfltV)(tp.asInstanceOf[PardisType[Any]])
  }

  /*
   * Generates local mutable variables instead of an array with a constant size.
   */
  rewrite += statement {
    case sym -> dsl"new Array[Any](${ Constant(v) })" if isConstSizeArray(sym) => {
      val elemType = sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      val vars = new Array[Var[Any]](v)
      val default = getDefaultValue(elemType)
      // TODO needs var support from quasi engine
      for (i <- 0 until v) {
        vars(i.toInt) = __newVar(default)(elemType)
      }
      arrayVars(sym) = vars
      default
    }
  }

  /**
   * Keeps the list of local mutable variables created for the array rewritten
   * using the previous rewrite rule
   */
  val arrayVars = scala.collection.mutable.Map[Rep[Any], Array[Var[Any]]]()

  // TODO needs more clear support from quasi engine
  /*
   * Converts a record with a constant size array field to a record with flattened
   * mutable fields representing the elements of that array.
   */
  rewrite += statement {
    case sym -> (ps @ Struct(tag, elems, methods)) if structHasSingletonArrayAsField(ps) =>
      val newElems = elems.flatMap(e =>
        if (e.init.tp.isArray && isConstSizeArray(e.init)) {
          // We know that every struct which as a constant array field with size > 1 has new Array has the init value
          // for that field
          val arraySize = constSizeArraysSize.get(e.init).getOrElse(1)
          structsConstSizeArrayFieldSize(sym.tp -> e.name) = arraySize
          if (arraySize > 1) {
            val init = getDefaultValue(e.init.tp.typeArguments(0))
            val newElems = for (index <- 0 until arraySize) yield {
              val newInit = readVar(__newVar(init)(init.tp))(init.tp)
              PardisStructArg(e.name + { if (index != 0) s"_$index" else "" }, true, newInit)
            }
            newElems.toList
          } else { // if (arraySize == 1) 
            val init = e.init match {
              case Constant(null) => getDefaultValue(e.init.tp.typeArguments(0))
              case _              => apply(e.init)
            }
            val newInit = readVar(__newVar(init)(init.tp))(init.tp)
            List(PardisStructArg(e.name, true, newInit))
          }
        } else List(e))
      toAtom(Struct(tag, newElems, methods)(ps.tp))(ps.tp)
  }

  /*
   * Whenever a constant size array which is one field of a record is updated,
   * it should be rewritten to setting the corresponding field of that record.
   */
  rewrite += rule {
    case dsl"((__struct_field($s, ${ Constant(f) }): Array[Any]) as $a)(${ Constant(i) }) = $v" if isConstSizeArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldSetter(s, s"$f$postFix", v)
  }

  /*
   * The same as above rule. Instead of updating an element, that element is accessed
   * and instead of setting the corresponding field, we should get the value of 
   * the corresponding field. 
   */
  rewrite += rule {
    case aa @ dsl"((__struct_field($s, ${ Constant(f) }): Array[Any]) as $a : Array[Any])(${ Constant(i) })" if isConstSizeArray(a) =>
      val postFix = if (i == 0) "" else s"_$i"
      fieldGetter(s, s"$f$postFix")(aa.tp)
  }

  /* The following two rules are very similar to the previous two rules.
   * The main difference is that in the following cases, the array is not a field
   * of a record, and is defined as local variable in the program. 
   * 
   * Furthermore, the order of the previous rules and the following rules should 
   * not be changed. Since, the following rules are always executed and as a reuslt
   * the two previous rules are always ignored.
   */

  rewrite += rule {
    case dsl"($a: Array[Any])(${ Constant(i) }) = $v" if isConstSizeArray(a) && arrayVars.contains(a) =>
      __assign(arrayVars(a)(i), v)
  }

  rewrite += rule {
    case dsl"($a: Array[Any])(${ Constant(i) })" if isConstSizeArray(a) && arrayVars.contains(a) =>
      readVar(arrayVars(a)(i))(a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }

  // TODO needs var support from quasi engine
  /*
   * In the case that a unit size array (an array with size 1) field of a record
   * is assigned to a variable, the variable should be rewritten to always get
   * the value of that singleton element.
   */
  rewrite += statement {
    case sym -> NewVar(a @ Def(StructImmutableField(s, f))) if isConstSizeArray(a) &&
      structsConstSizeArrayFieldSize(s.tp, f) == 1 =>
      val elemType = a.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      val res = __newVar(fieldGetter(s, f)(elemType))(elemType)
      escapedVarArrays(sym) = res
      res.e
  }

  /**
   * Keeps the mapping between the old mutable variable and the new mutable variable
   * created by the previous rewrite rule.
   */
  val escapedVarArrays = scala.collection.mutable.Map[Rep[Any], Var[Any]]()

  // TODO needs var support from quasi engine
  /*
   * For mutable variables rewritten using the previous rewrite rule, the read 
   * accesses should be substituted by the reading from the rewritten variable.
   */
  rewrite += rule {
    case ReadVar(v) if escapedVarArrays.contains(v.e) =>
      val newV = escapedVarArrays(v.e)
      readVar(newV)(newV.e.tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
  }
}
