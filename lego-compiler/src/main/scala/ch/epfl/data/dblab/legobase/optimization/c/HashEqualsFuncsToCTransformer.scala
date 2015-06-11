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
 * Transforms the object methods into their corresponding C implementation.
 *
 * The supported object operations (for now) are `hashCode`, `equals`, and `toString`
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 */
class HashEqualsFuncsToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer with StructCollector[LoweringLegoBase] {
  import IR._
  import CNodes._
  import CTypes._

  // Handling proper hascode and equals
  def __isRecord(e: Expression[Any]) = e.tp.isRecord || (e.tp.isPointerType && e.tp.typeArguments(0).isRecord)
  object Equals {
    def unapply(node: Def[Any]): Option[(Rep[Any], Rep[Any], Boolean)] = node match {
      case Equal(a, b)    => Some((a, b, true))
      case NotEqual(a, b) => Some((a, b, false))
      case _              => None
    }
  }

  val alreadyEquals = scala.collection.mutable.ArrayBuffer[Rep[Any]]()

  rewrite += rule {
    case Equals(e1, Constant(null), isEqual) if (e1.tp == OptimalStringType || e1.tp == StringType) && !alreadyEquals.contains(e1) =>
      alreadyEquals += e1
      if (isEqual) (e1 __== unit(null)) || !strcmp(e1, unit("")) else (e1 __!= unit(null)) && strcmp(e1, unit(""))
    case Equals(e1, e2, isEqual) if (e1.tp == OptimalStringType || e1.tp == StringType) && !alreadyEquals.contains(e1) =>
      if (isEqual) !strcmp(e1, e2) else strcmp(e1, e2)
    case Equals(e1, Constant(null), isEqual) if __isRecord(e1) && !alreadyEquals.contains(e1) =>
      val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0))
      val structDef = if (e1.tp.isRecord)
        getStructDef(ttp).get
      else {
        try {
          getStructDef(ttp).get
        } catch {
          case ex => throw new Exception(s"${e1.tp} is not a record type in $e1 and the node ${e1.correspondingNode} => ${ex}")
        }
      }
      // System.out.println(structDef.fields)
      // System.out.println(s"HERE for $e1:${e1.tp}")
      alreadyEquals += e1
      // FIMXE HACK for Q13
      if (ttp.name == "AGGRecord_Double" && structDef.fields.exists(f => f.name == "aggs" && f.tpe == DoubleType)) {
        // System.out.println(s"here for $e1")
        def fieldExp = field[Double](e1, "aggs")
        if (isEqual)
          (e1 __== unit(null)) || (fieldExp __== unit(0))
        else
          (e1 __!= unit(null)) && (fieldExp __!= unit(0))
      } else {
        structDef.fields.filter(_.name != "next").find(f => f.tpe.isPointerType || f.tpe == OptimalStringType || f.tpe == StringType) match {
          case Some(firstField) =>
            def fieldExp = field(e1, firstField.name)(firstField.tpe)
            if (isEqual)
              (e1 __== unit(null)) || (fieldExp __== unit(null))
            else
              (e1 __!= unit(null)) && (fieldExp __!= unit(null))
          case None => {
            if (isEqual)
              (e1 __== unit(null))
            else
              (e1 __!= unit(null))
          }
        }
      }
    // case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
    //   class T
    //   implicit val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0)).asInstanceOf[TypeRep[T]]
    //   val eq = getStructEqualsFunc[T]()
    //   val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
    //   if (isEqual) res else !res
    case Equals(e1, e2, isEqual) if __isRecord(e1) && __isRecord(e2) =>
      val ttp = (if (e1.tp.isRecord) e1.tp else e1.tp.typeArguments(0))
      val structDef = getStructDef(ttp).get
      val res = if (ttp.name == "Q10GRPRecord") {
        field[Int](e1, "C_CUSTKEY") __== field[Int](e2, "C_CUSTKEY")
      } else {
        structDef.fields.filter(_.name != "next").map(f => field(e1, f.name)(f.tpe) __== field(e2, f.name)(f.tpe)).reduce(_ && _)
      }
      // val res = structDef.fields.filter(_.name != "next").map(f => field(e1, f.name)(f.tpe) __== field(e2, f.name)(f.tpe)).reduce(_ && _)
      // val eq = getStructEqualsFunc[T]()
      // val res = inlineFunction(eq, e1.asInstanceOf[Rep[T]], e2.asInstanceOf[Rep[T]])
      if (isEqual) res else !res
  }
  rewrite += rule {
    case HashCode(t) if t.tp == StringType => unit(0) // KEY is constant. No need to hash anything
    // case HashCode(t) if __isRecord(t) =>
    //   val tp = t.tp.asInstanceOf[PardisType[Any]]
    //   val hashFunc = {
    //     if (t.tp.isRecord) getStructHashFunc[Any]()(tp)
    //     else getStructHashFunc[Any]()(tp.typeArguments(0).asInstanceOf[TypeRep[Any]])
    //   }
    //   val hf = toAtom(hashFunc)(hashFunc.tp)
    //   inlineFunction(hf.asInstanceOf[Rep[Any => Int]], apply(t))
    case HashCode(e) if __isRecord(e) =>
      val ttp = (if (e.tp.isRecord) e.tp else e.tp.typeArguments(0))
      val structDef = getStructDef(ttp).get
      if (ttp.name == "Q9GRPRecord" && structDef.fields.forall(_.tpe == IntType)) {
        field[Int](e, "NATION") * unit(10000) + field[Int](e, "O_YEAR")
      } else if (ttp.name == "Q16GRPRecord2" && structDef.fields.forall(_.tpe == IntType)) {
        field[Int](e, "P_BRAND") * unit(10000) + field[Int](e, "P_TYPE") * unit(100) + field[Int](e, "P_SIZE")
      } else if (ttp.name == "Q10GRPRecord") {
        field[Int](e, "C_CUSTKEY")
      } else {
        structDef.fields.map(f => infix_hashCode(field(e, f.name)(f.tpe))).reduce(_ + _)
      }

    case HashCode(t) if t.tp.isPrimitive      => infix_asInstanceOf[Int](t)
    case HashCode(t) if t.tp == CharacterType => infix_asInstanceOf[Int](t)
    case HashCode(t) if t.tp == OptimalStringType =>
      val len = t.asInstanceOf[Expression[OptimalString]].length
      val idx = __newVar[Int](0)
      val h = __newVar[Int](0)
      __whileDo(readVar(idx) < len, {
        __assign(h, readVar(h) + OptimalStringApply(t.asInstanceOf[Expression[OptimalString]], readVar(idx)))
        __assign(idx, readVar(idx) + unit(1));
      })
      readVar(h)
    case HashCode(t) if t.tp.isArray => field[Int](t.asInstanceOf[Rep[Array[Any]]], "length")
    //case HashCode(t)                 => throw new Exception("Unhandled type " + t.tp.toString + " passed to HashCode")
  }
  rewrite += rule {
    case ToString(obj) => obj.tp.asInstanceOf[PardisType[_]] match {
      case PointerType(tpe) if tpe.isRecord => {
        val structFields = {
          val structDef = getStructDef(tpe).get
          structDef.fields
        }
        def getDescriptor(field: StructElemInformation): String = field.tpe.asInstanceOf[PardisType[_]] match {
          case IntType | ShortType            => "%d"
          case DoubleType | FloatType         => "%f"
          case LongType                       => "%lf"
          case StringType | OptimalStringType => "%s"
          case ArrayType(elemTpe)             => s"Array[$elemTpe]"
          case tp                             => tp.toString
        }
        val fieldsWithDescriptor = structFields.map(f => f -> getDescriptor(f))
        val descriptor = tpe.name + "(" + fieldsWithDescriptor.map(f => f._2).mkString(", ") + ")"
        val fields = fieldsWithDescriptor.collect {
          case f if f._2.startsWith("%") => {
            val tp = f._1.tpe
            field(obj, f._1.name)(tp)
          }
        }
        val str = malloc(4096)(CharType)
        sprintf(str.asInstanceOf[Rep[String]], unit(descriptor), fields: _*)
        str.asInstanceOf[Rep[String]]
      }
      case tp => throw new Exception(s"toString conversion for non-record type $tp is not handled for the moment")
    }
  }
}
