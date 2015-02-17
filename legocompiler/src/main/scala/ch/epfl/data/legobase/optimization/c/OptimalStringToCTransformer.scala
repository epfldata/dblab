package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.types._
import scala.language.implicitConversions

class OptimalStringToCTransformer(override val IR: LoweringLegoBase) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._
  import cscala.CLangTypes

  private[OptimalStringToCTransformer] implicit def optimalStringToCharPointer(x: Rep[OptimalString]) = x.asInstanceOf[Rep[LPointer[Char]]]

  implicit class OptimalStringRep(self: Rep[OptimalString]) {
    def getBaseValue(s: Rep[OptimalString]): Rep[LPointer[Char]] = apply(s).asInstanceOf[Rep[LPointer[Char]]]
  }

  rewrite += rule { case OptimalStringNew(self) => self }
  rewrite += rule {
    case OptimalStringString(self) =>
      new OptimalStringRep(self).getBaseValue(self)
  }
  rewrite += rule {
    case OptimalStringDiff(self, y) =>
      CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y))
  }
  rewrite += rule {
    case OptimalStringEndsWith(self, y) =>
      {
        val lenx: Rep[ch.epfl.data.cscala.CLangTypes.CSize] = CString.strlen(new OptimalStringRep(self).getBaseValue(self));
        val leny: Rep[ch.epfl.data.cscala.CLangTypes.CSize] = CString.strlen(new OptimalStringRep(self).getBaseValue(y));
        val len: Rep[Int] = lenx.$minus(leny);
        infix_$eq$eq(CString.strncmp(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), len)(typeRep[Char], CLangTypes.charType), new OptimalStringRep(self).getBaseValue(y), len), unit(0))
      }
  }
  rewrite += rule {
    case OptimalStringStartsWith(self, y) =>
      infix_$eq$eq(CString.strncmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y), CString.strlen(new OptimalStringRep(self).getBaseValue(y))), unit(0))
  }
  rewrite += rule {
    case OptimalStringCompare(self, y) =>
      CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y))
  }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(x, y) }
  rewrite += rule { case OptimalStringCompare(x, y) => strcmp(x, y) }
  rewrite += rule { case OptimalStringLength(x) => strlen(x) }
  rewrite += rule { case OptimalString$eq$eq$eq(x, y) => strcmp(x, y) __== unit(0) }
  rewrite += rule { case OptimalString$eq$bang$eq(x, y) => infix_!=(strcmp(x, y), unit(0)) }
  rewrite += rule { case OptimalStringContainsSlice(x, y) => infix_!=(strstr(x, y), unit(null)) }
  rewrite += rule {
    case OptimalStringLength(self) =>
      CString.strlen(new OptimalStringRep(self).getBaseValue(self))
  }
  rewrite += rule {
    case OptimalString$eq$eq$eq(self, y) =>
      infix_$eq$eq(CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), unit(0))
  }
  rewrite += rule {
    case OptimalString$eq$bang$eq(self, y) =>
      infix_$bang$eq(CString.strcmp(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), unit(0))
  }
  rewrite += rule {
    case OptimalStringContainsSlice(self, y) =>
      infix_$bang$eq(CString.strstr(new OptimalStringRep(self).getBaseValue(self), new OptimalStringRep(self).getBaseValue(y)), CLang.NULL[Char])
  }
  rewrite += rule {
    case OptimalStringIndexOfSlice(self, y, idx) =>
      {
        val substr: Rep[ch.epfl.data.cscala.CLangTypes.LPointer[Char]] = CString.strstr(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), idx)(typeRep[Char], CLangTypes.charType), new OptimalStringRep(self).getBaseValue(y));
        __ifThenElse(infix_$eq$eq(substr, CLang.NULL[Char]), unit(-1), CString.str_subtract(substr, new OptimalStringRep(self).getBaseValue(self)))
      }
  }
  rewrite += rule {
    case OptimalStringApply(self, idx) =>
      CLang.*(CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), idx)(typeRep[Char], CLangTypes.charType))
  }
  rewrite += rule {
    case OptimalStringSlice(self, start, end) =>
      {
        val len: Rep[Int] = end.$minus(start).$plus(unit(1));
        //FIXME CStdLib.malloc should be generated in a proper way
        val newbuf: Rep[ch.epfl.data.cscala.CLangTypes.LPointer[Char]] = CStdLib.malloc[Char](len);
        CString.strncpy(newbuf, CLang.pointer_add[Char](new OptimalStringRep(self).getBaseValue(self), start)(typeRep[Char], CLangTypes.charType), len.$minus(unit(1)));
        pointer_assign_content(newbuf.asInstanceOf[Expression[Pointer[Any]]], len - 1, unit('\u0000'))
        newbuf
      }
  }
  rewrite += rule {
    case OptimalStringReverse(x) =>
      val len: Rep[Int] = strlen(apply(x)) + 1
      val newbuf: Rep[ch.epfl.data.cscala.CLangTypes.LPointer[Char]] = CStdLib.malloc[Char](len);
      CString.strncpy(newbuf, new OptimalStringRep(x).getBaseValue(x), len.$minus(unit(1)));
      pointer_assign_content(newbuf.asInstanceOf[Expression[Pointer[Any]]], len - 1, unit('\u0000'))

      val str = infix_asInstanceOf(newbuf)(typeArray(typePointer(CharType))).asInstanceOf[Rep[Array[Pointer[Char]]]]
      val i = __newVar[Int](unit(0))
      val j = __newVar[Int](strlen(str) - 1)

      __whileDo((i: Rep[Int]) < (j: Rep[Int]), {
        val _i = (i: Rep[Int])
        val _j = (j: Rep[Int])
        val temp = str(_i)
        str(_i) = str(_j)
        str(_j) = temp
        __assign(i, _i + unit(1))
        __assign(j, _j - unit(1))
      })
      str
  }
}
