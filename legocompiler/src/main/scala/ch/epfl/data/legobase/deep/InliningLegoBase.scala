package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._
import pardis.types.PardisTypeImplicits._

// trait InliningLegoBase extends volcano.InliningVolcano with DeepDSL with pardis.ir.InlineFunctions with QueriesImplementations with LoopUnrolling with InliningLoader
trait InliningLegoBase extends push.InliningPush with DeepDSL with pardis.ir.InlineFunctions with QueriesImplementations with LoopUnrolling with InliningLoader

trait InliningLoader extends LoaderImplementations { this: InliningLegoBase =>

  override def loaderGetFullPathObject(fileName: Rep[String]): Rep[String] = fileName match {
    case Constant(name: String) => unit(Config.datapath + name)
    case _                      => throw new Exception(s"file name should be constant but here it is $fileName")
  }
  // override def loaderLoadLineitemObject(): Rep[Array[LINEITEMRecord]] = {
  //   val file = unit(Config.datapath + "lineitem.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](unit[Int](0))
  //   val hm = __newArray[LINEITEMRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newLINEITEMRecord(
  //       s.next_int,
  //       s.next_int,
  //       s.next_int,
  //       s.next_int,
  //       s.next_double,
  //       s.next_double,
  //       s.next_double,
  //       s.next_double,
  //       s.next_char,
  //       s.next_char,
  //       s.next_date,
  //       s.next_date,
  //       s.next_date,
  //       loadString(25, s),
  //       loadString(10, s),
  //       loadString(44, s))
  //     hm.update(readVar(i), newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadSupplierObject(): Rep[Array[SUPPLIERRecord]] = {
  //   val file = unit(Config.datapath + "supplier.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[SUPPLIERRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newSUPPLIERRecord(
  //       s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(101, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadPartsuppObject(): Rep[Array[PARTSUPPRecord]] = {
  //   val file = unit(Config.datapath + "partsupp.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[PARTSUPPRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newPARTSUPPRecord(s.next_int, s.next_int, s.next_int, s.next_double, loadString(199, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadCustomerObject(): Rep[Array[CUSTOMERRecord]] = {
  //   val file = unit(Config.datapath + "customer.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[CUSTOMERRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newCUSTOMERRecord(s.next_int, loadString(25, s), loadString(40, s), s.next_int, loadString(15, s), s.next_double, loadString(10, s), loadString(117, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadOrdersObject(): Rep[Array[ORDERSRecord]] = {
  //   val file = unit(Config.datapath + "orders.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[ORDERSRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newORDERSRecord(s.next_int, s.next_int, s.next_char, s.next_double, s.next_date,
  //       loadString(15, s), loadString(15, s), s.next_int, loadString(79, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadNationObject(): Rep[Array[NATIONRecord]] = {
  //   val file = unit(Config.datapath + "nation.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[NATIONRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newNATIONRecord(s.next_int, loadString(25, s), s.next_int, loadString(152, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadPartObject(): Rep[Array[PARTRecord]] = {
  //   val file = unit(Config.datapath + "part.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[PARTRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newPARTRecord(s.next_int, loadString(55, s), loadString(25, s), loadString(10, s), loadString(25, s),
  //       s.next_int, loadString(10, s), s.next_double, loadString(23, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // override def loaderLoadRegionObject(): Rep[Array[REGIONRecord]] = {
  //   val file = unit(Config.datapath + "region.tbl")
  //   val size = Loader.fileLineCount(file)
  //   // Load Relation 
  //   val s = __newK2DBScanner(file)
  //   var i = __newVar[Int](0)
  //   val hm = __newArray[REGIONRecord](size)
  //   __whileDo(s.hasNext, {
  //     val newEntry = __newREGIONRecord(s.next_int, loadString(25, s), loadString(152, s))
  //     hm.update(i, newEntry)
  //     __assign(i, readVar(i) + unit(1))
  //     unit()
  //   })
  //   hm
  // }

  // def loadString(size: Rep[Int], s: Rep[K2DBScanner]) = {
  //   val NAME = __newArray[Byte](size)
  //   s.next(NAME)
  //   __newOptimalString(NAME.filter(__lambda(y => infix_!=(y, unit(0)))))
  // }
}

trait LoopUnrolling extends pardis.ir.InlineFunctions { this: InliningLegoBase =>
  override def seqForeach[A, U](self: Rep[Seq[A]], f: Rep[A => U])(implicit typeA: TypeRep[A], typeU: TypeRep[U]): Rep[Unit] = self match {
    case Def(LiftedSeq(elems)) => elems.toList match {
      case Nil => unit(())
      case elem :: tail => {
        __app(f).apply(elem)
        __liftSeq(tail).foreach(f)
      }
    }
  }
}

