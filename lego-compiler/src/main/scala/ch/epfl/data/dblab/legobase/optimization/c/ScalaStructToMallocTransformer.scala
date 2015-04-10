package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import scala.language.existentials

/**
 * Transforms `new Struct` in Scala to `malloc(..)` in C.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ScalaStructToMallocTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case c if c.isPrimitive    => super.transformType[T]
      case ArrayBufferType(args) => typePointer(typeGArray)
      // case ArrayType(x) if x == ByteType => typePointer(ByteType)
      // case ArrayType(args) => typePointer(typeCArray({
      //   if (args.isArray) typeCArray(args)
      //   else args
      // }))
      case ArrayType(args)       => typePointer(args)
      // case c if c.isRecord => tp.typeArguments match {
      //   case Nil     => typePointer(tp)
      //   case List(t) => typePointer(transformType(t))
      // }
      case TreeSetType(args)     => typePointer(typeGTree)
      case OptionType(args)      => typePointer(transformType(args))
      case _                     => super.transformType[T]
    }
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case pc @ PardisCast(x) => PardisCast(apply(x))(apply(pc.castFrom), apply(pc.castTp))
  }

  rewrite += rule {
    case s @ PardisStruct(tag, elems, methods) =>
      // TODO if needed method generation should be added
      val x = toAtom(Malloc(unit(1))(s.tp))(typePointer(s.tp))
      val newElems = elems.map(el => PardisStructArg(el.name, el.mutable, transformExp(el.init)(el.init.tp, apply(el.init.tp))))
      structCopy(x, PardisStruct(tag, newElems, methods.map(m => m.copy(body =
        transformDef(m.body.asInstanceOf[Def[Any]]).asInstanceOf[PardisLambdaDef])))(s.tp))
      x
  }
}
