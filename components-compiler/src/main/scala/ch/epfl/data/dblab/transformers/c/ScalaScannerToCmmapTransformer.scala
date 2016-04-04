package ch.epfl.data
package dblab
package transformers
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import scala.language.implicitConversions

/**
 * Transforms Scala Scanner operations to C mmap operations.
 *
 * @param IR the polymorphic embedding trait which contains the reified program.
 * @param settings the compiler settings provided as command line arguments (TODO should be removed)
 */
class ScalaScannerToCmmapTransformer(override val IR: QueryEngineExp, val settings: CTransformersPipelineSettings) extends RuleBasedTransformer[QueryEngineExp](IR) {
  import IR._
  import CNodes._
  import CTypes._

  implicit def toArrayChar(s: Expression[FastScanner]): Expression[Pointer[Char]] =
    s.asInstanceOf[Expression[Pointer[Char]]]

  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp == typeFastScanner) typePointer(typeChar)
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  /* Helper functions */
  def readInt(s: Expression[Pointer[Char]]): Expression[Int] = {
    val num = readVar(__newVar[Int](unit(0)))
    pointer_assign(s,
      toAtom(NameAlias[Pointer[Char]](None, "strntoi_unchecked", List(List(s, &(num))))))
    num
  }

  // rewritings
  rewrite += rule {
    case FastScannerNew(f) => {
      val fd: Expression[Int] = open(f, O_RDONLY)
      val st = &(__newVar[StructStat](infix_asInstanceOf(unit(0))(typeStat)))
      stat(f, st)
      val size = field(st, "st_size")(typeSize_T)
      NameAlias[Pointer[Char]](None, "mmap", List(List(Constant(null), size, PROT_READ, MAP_PRIVATE, fd, Constant(0))))
    }
  }

  rewrite += rule { case FastScannerNext_int(s) => readInt(s) }

  rewrite += rule {
    case FastScannerNext_double(s) =>
      val num = readVar(__newVar[Double](unit(0.0)))
      pointer_assign(s,
        toAtom(NameAlias[Pointer[Char]](None, "strntod_unchecked", List(List(s, &(num))))))
      num
  }

  rewrite += rule {
    case FastScannerNext_char(s) =>
      val tmp = __newVar[Char](*(s));
      pointer_increase(s) // move to next char, which is the delimiter
      pointer_increase(s) // skip '|'
      readVar(tmp)
  }

  rewrite += rule {
    case FastScannerNext_date(s) => {
      val year = readInt(s)
      val month = readInt(s)
      val day = readInt(s)
      (year * unit(10000)) + (month * unit(100)) + day
    }
  }

  rewrite += rule {
    case nn @ FastScannerNext1(s, buf) =>
      // val array = field(buf, "array")(typePointer(typeChar))
      // val array = buf.asInstanceOf[Rep[Pointer[Char]]]
      val array = if (settings.oldCArrayHandling)
        field[Pointer[Char]](buf, "array")(typePointer(typeChar))
      else
        buf.asInstanceOf[Rep[Pointer[Char]]]

      val begin = __newVar[Pointer[Char]](s)
      __whileDo((*(s) __!= unit('|')) && (*(s) __!= unit('\n')), {
        pointer_increase(s)
      })
      val size = pointer_minus(s, readVar(begin))
      strncpy(array, readVar(begin), size)
      // pointer_assign_content(array, size, unit('\u0000'))
      // buf(size) = unit('\u0000')
      if (settings.oldCArrayHandling)
        pointer_assign_content(array, size, unit('\u0000'))
      else
        // buf(size - 1) = unit('\u0000')
        // buf(size) = unit('\u0000')
        ()
      pointer_increase(s) // skip '|'
      size
  }

  rewrite += rule {
    case nn @ FastScannerNext_string(s) =>
      val begin = __newVar[Pointer[Char]](s)
      __whileDo((*(s) __!= unit('|')) && (*(s) __!= unit('\n')), {
        pointer_increase(s)
      })
      val size = pointer_minus(s, readVar(begin))
      val array = __newOptimalString(__newArray[Byte](size + unit(1))).asInstanceOf[Rep[Pointer[Char]]]
      strncpy(array, readVar(begin), size)
      // pointer_assign_content(array, size, unit('\u0000'))
      // buf(size) = unit('\u0000')
      if (settings.oldCArrayHandling)
        pointer_assign_content(array, size, unit('\u0000'))
      else
        // buf(size - 1) = unit('\u0000')
        // buf(size) = unit('\u0000')
        ()
      pointer_increase(s) // skip '|'
      array
  }

  rewrite += rule {
    case FastScannerHasNext(s) => infix_!=(*(s), unit('\u0000'))

  }
  rewrite += rule {
    case LoaderFileLineCountObject(Constant(x: String)) =>
      val p = popen(unit("wc -l " + x), unit("r"))
      val cnt = readVar(__newVar[Int](unit(0)))
      fscanf(p, unit("%d"), &(cnt))
      pclose(p)
      cnt
  }
}
