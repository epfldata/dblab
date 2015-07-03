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
import sc.pardis.deep.scalalib.collection._

/**
 * Factory for creating instances of [[ContainerFlatTransformer]]
 */
object ContainerFlatTransformer extends TransformerHandler {
  def apply[Lang <: Base, T: PardisType](context: Lang)(block: context.Block[T]): context.Block[T] = {
    new ContainerFlatTransformer(context.asInstanceOf[LoweringLegoBase]).optimize(block)
  }
}

// FIXME check if EscapeLowering can be used here
/**
 * A transformer which flattens the next field of a container of a record to the record itself
 *
 * This transformer is useful when a linked-list is used.
 *
 * Example:
 * {{{
 *    // class Container[T] { val elem: T, var next: Container[T] }
 *    // struct RecordA { val fieldA: Int, val fieldB: String }
 *    val rec1 = new RecordA(...)
 *    val rec2 = new RecordA(...)
 *    val contRecA = new Containter[RecordA](rec1)
 *    contRecA.next = new Containter[RecordA](rec2)
 *    val rec3: RecordA = contRecA.elem
 *    process(rec3)
 * }}}
 * is converted to:
 * {{{
 *    // RecordA { val fieldA: Int, val fieldB: String, var next: RecordA }
 *    val rec1 = new RecordA(...)
 *    val rec2 = new RecordA(...)
 *    rec1.next = rec2
 *    process(rec1)
 * }}}
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class ContainerFlatTransformer[Lang <: sc.pardis.deep.scalalib.ArrayComponent with sc.pardis.deep.scalalib.Tuple2Component with ContOps](override val IR: Lang)
  extends sc.pardis.optimization.RecursiveRuleBasedTransformer[Lang](IR)
  with StructCollector[Lang] {
  import IR._
  type Rep[T] = IR.Rep[T]
  type Var[T] = IR.Var[T]

  val recordTypes = scala.collection.mutable.Set[TypeRep[Any]]()

  def mayBeLowered[T](sym: Rep[T]): Boolean =
    isContRecord(sym.tp)

  def mustBeLowered[T](node: Def[T]): Boolean =
    recordTypes.exists(x => x == node.tp) // && getStructDef(sym.tp).get.fields.forall(f => f.name != "next")

  def isContRecord[T](tp: TypeRep[T]): Boolean = tp match {
    case ContType(t) if t.isRecord => true
    case _                         => false
  }

  def addSymRecordType[T](sym: Rep[T]): Unit =
    if (isContRecord(sym.tp)) {
      val tp = sym.tp.typeArguments(0).asInstanceOf[TypeRep[Any]]
      // System.out.println(s"tp $tp added")
      recordTypes += tp
    }

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    tp match {
      case ContType(t) if t.isRecord => t
      case _                         => super.transformType(tp)
    }
  }).asInstanceOf[PardisType[Any]]

  analysis += statement {
    case sym -> (node @ ContNew(elem, next)) if mayBeLowered(sym) =>
      addSymRecordType(sym)
  }

  analysis += rule {
    case Cont_Field_Next__eq(self, x) if mayBeLowered(self) =>
      addSymRecordType(self)
  }

  analysis += rule {
    case Cont_Field_Next(self) if mayBeLowered(self) =>
      addSymRecordType(self)
  }

  analysis += rule {
    case Cont_Field_Elem(self) if mayBeLowered(self) =>
      addSymRecordType(self)
  }

  rewrite += statement {
    case sym -> (node @ ContNew(elem, next)) if mayBeLowered(sym) =>
      elem match {
        case Constant(null) =>
        case _              => fieldSetter(elem, "next", next)
      }
      elem
  }

  rewrite += rule {
    case Cont_Field_Next__eq(self, x) if mayBeLowered(self) =>
      fieldSetter(self, "next", x)
  }

  rewrite += rule {
    case n @ Cont_Field_Next(self) if mayBeLowered(self) =>
      fieldGetter(self, "next")(apply(n.tp))
  }

  rewrite += rule {
    case Cont_Field_Elem(self) if mayBeLowered(self) =>
      self
  }

  // analysis += statement {
  //   case sym -> (node @ Struct(tag, elems, methods)) /*if mustBeLowered(sym) && elems.forall(f => f.name != "next")*/ =>
  //     System.out.println(s"found next for ${node.tp}, $sym, ${sym.tp}")
  //     // IR.asInstanceOf[LoweringLegoBase].printf(unit(s"struct creation $tag: ${node.tp}"))
  //     ()
  // }

  rewrite += statement {
    case sym -> (node @ Struct(tag, elems, methods)) if mustBeLowered(node) && elems.forall(f => f.name != "next") =>
      // System.out.println(s"appending next to ${node.tp}, $sym")
      // IR.asInstanceOf[LoweringLegoBase].printf(unit(s"struct creation $tag: ${node.tp}"))
      toAtom(Struct(tag, elems :+ PardisStructArg("next", true, infix_asInstanceOf(unit(null))(node.tp)), methods)(node.tp))(node.tp)
  }

  analysis += statement {
    case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
      recordTypes += sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[Any]]
      ()
    }
  }

  rewrite += statement {
    case sym -> ArrayNew(len) if isContRecord(sym.tp.typeArguments(0)) => {
      class B
      implicit val typeB = sym.tp.typeArguments(0).typeArguments(0).asInstanceOf[TypeRep[B]]
      __newArray[B](len).asInstanceOf[Rep[Any]]
    }
  }

  analysis += statement {
    case sym -> Tuple2New(_1, _2) if isContRecord(sym.tp.typeArguments(1)) => {
      // System.out.println(s"tuple 2 new lowering ${_2}")
      recordTypes += sym.tp.typeArguments(1).typeArguments(0).asInstanceOf[TypeRep[Any]]
      ()
    }
  }

  rewrite += statement {
    case sym -> Tuple2New(node_1, node_2) if isContRecord(sym.tp.typeArguments(1)) => {
      class A
      class B
      val _1 = node_1.asInstanceOf[Rep[A]]
      val _2 = node_2.asInstanceOf[Rep[B]]

      implicit val typeA = node_1.tp.asInstanceOf[TypeRep[A]]
      implicit val typeB = node_2.tp.typeArguments(0).asInstanceOf[TypeRep[B]]
      // System.out.println(s"tuple 2 lowering ${typeB}")
      __newTuple2(_1, _2).asInstanceOf[Rep[Any]]
    }
  }

  rewrite += statement {
    case sym -> (node @ Cast(v)) if isContRecord(sym.tp) =>
      infix_asInstanceOf(v)(apply(sym.tp))
  }
}
