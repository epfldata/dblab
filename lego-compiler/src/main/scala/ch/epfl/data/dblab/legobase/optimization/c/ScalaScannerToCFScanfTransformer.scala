package ch.epfl.data
package dblab.legobase
package optimization
package c

import deep._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import sc.cscala.CLangTypesDeep._
import sc.cscala.GLibTypes._

// Mapping Scala Scanner operations to C FILE operations
class ScalaScannerToCFScanfTransformer(override val IR: LoweringLegoBase) extends RuleBasedTransformer[LoweringLegoBase](IR) {
  import IR._
  import CNodes._
  import CTypes._

  // TODO: Brainstorm about rewrite += tp abstraction for transforming types
  override def transformType[T: PardisType]: PardisType[Any] = ({
    val tp = typeRep[T]
    if (tp == typeK2DBScanner) typePointer(typeFILE)
    else super.transformType[T]
  }).asInstanceOf[PardisType[Any]]

  rewrite += rule {
    case K2DBScannerNew(f) => CStdIO.fopen(f.asInstanceOf[Rep[LPointer[Char]]], unit("r"))
  }
  rewrite += rule {
    case K2DBScannerNext_int(s) =>
      val v = readVar(__newVar[Int](0))
      __ifThenElse(fscanf(apply(s), unit("%d|"), &(v)) __== eof, break, unit(()))
      v
  }
  rewrite += rule {
    case K2DBScannerNext_double(s) =>
      val v = readVar(__newVar(unit(0.0)))
      __ifThenElse(fscanf(apply(s), unit("%lf|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case K2DBScannerNext_char(s) =>
      val v = readVar(__newVar(unit('a')))
      __ifThenElse(fscanf(apply(s), unit("%c|"), &(v)) __== eof, break, unit)
      v
  }
  rewrite += rule {
    case nn @ K2DBScannerNext1(s, buf) =>
      var i = __newVar[Int](0)
      __whileDo(unit(true), {
        val v = readVar(__newVar[Byte](unit('a')))
        __ifThenElse(fscanf(apply(s), unit("%c"), &(v)) __== eof, break, unit)
        // have we found the end of line or end of string?
        __ifThenElse((v __== unit('|')) || (v __== unit('\n')), break, unit)
        buf(i) = v
        __assign(i, readVar(i) + unit(1))
      })
      buf(i) = unit('\u0000')
      readVar(i)
  }
  rewrite += rule {
    case K2DBScannerNext_date(s) =>
      val x = readVar(__newVar[Int](unit(0)))
      val y = readVar(__newVar[Int](unit(0)))
      val z = readVar(__newVar[Int](unit(0)))
      __ifThenElse(fscanf(apply(s), unit("%d-%d-%d|"), &(x), &(y), &(z)) __== eof, break, unit)
      (x * unit(10000)) + (y * unit(100)) + z
  }
  rewrite += rule { case K2DBScannerHasNext(s) => unit(true) }
  rewrite += rule {
    case LoaderFileLineCountObject(Constant(x: String)) =>
      val p = CStdIO.popen(unit("wc -l " + x), unit("r"))
      val cnt = readVar(__newVar[Int](0))
      CStdIO.fscanf(p, unit("%d"), CLang.&(cnt))
      CStdIO.pclose(p)
      cnt
  }
}
