package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

trait InliningLegoBase extends DeepDSL with pardis.ir.InlineFunctions with LoopUnrolling with OperatorImplementations with ScanOpImplementations with SelectOpImplementations with AggOpImplementations with SortOpImplementations with MapOpImplementations with PrintOpImplementations {
  def reifyInline[T: Manifest](e: => Rep[T]): Rep[T] = e

  override def operatorOpen[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = self match {
    case Def(x: PrintOpNew[_]) => self.asInstanceOf[Rep[PrintOp[A]]].open
    case Def(x: SortOpNew[_])  => self.asInstanceOf[Rep[SortOp[A]]].open
    case Def(x: MapOpNew[_])   => self.asInstanceOf[Rep[MapOp[A]]].open
    case Def(x: AggOpNew[_, _]) => {
      type X = A
      type Y = Any
      aggOpOpen(self.asInstanceOf[Rep[AggOp[X, Y]]])(x.manifestA.asInstanceOf[Manifest[X]], x.manifestB.asInstanceOf[Manifest[Y]])
    }
    case Def(x: ScanOpNew[_])   => self.asInstanceOf[Rep[ScanOp[A]]].open
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].open
    case _                      => super.operatorOpen(self)
  }

  override def operatorNext[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[A] = self match {
    case Def(x: PrintOpNew[_]) => self.asInstanceOf[Rep[PrintOp[A]]].next
    case Def(x: SortOpNew[_])  => self.asInstanceOf[Rep[SortOp[A]]].next
    case Def(x: MapOpNew[_])   => self.asInstanceOf[Rep[MapOp[A]]].next
    case Def(x: AggOpNew[_, _]) => {
      type X = A
      type Y = Any
      aggOpNext(self.asInstanceOf[Rep[AggOp[X, Y]]])(x.manifestA.asInstanceOf[Manifest[X]], x.manifestB.asInstanceOf[Manifest[Y]]).asInstanceOf[Rep[A]]
    }
    case Def(x: ScanOpNew[_])   => self.asInstanceOf[Rep[ScanOp[A]]].next
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].next
    case _                      => super.operatorNext(self)
  }

  override def printOp_Field_Parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: PrintOpNew[_]) => x.parent
      case _                     => super.printOp_Field_Parent(self)
    }
  }

  override def sortOp_Field_Parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SortOpNew[_]) => x.parent
      case _                    => super.sortOp_Field_Parent(self)
    }
  }

  override def mapOp_Field_Parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: MapOpNew[_]) => x.parent
      case _                   => super.mapOp_Field_Parent(self)
    }
  }

  override def selectOp_Field_Parent[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SelectOpNew[_]) => x.parent
      case _                      => super.selectOp_Field_Parent(self)
    }
  }

  override def aggOp_Field_Parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = {
    self match {
      case Def(x: AggOpNew[_, _]) => x.parent
      case _                      => super.aggOp_Field_Parent(self)
    }
  }

  override def mapOp_Field_AggFuncs[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Seq[A => Unit]] = self match {
    case Def(MapOpNew(_, funs)) => funs
    case _                      => super.mapOp_Field_AggFuncs(self)
  }

  override def aggOp_Field_Grp[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[A => B] = self match {
    case Def(AggOpNew(_, _, f, _)) => f
    case _                         => ???
  }
  override def aggOp_Field_AggFuncs[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Seq[((A, Double) => Double)]] = self match {
    case Def(AggOpNew(_, _, _, funs)) => funs
    case _                            => ???
  }

  override def printOp_Field_PrintFunc[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A => Unit] = self match {
    case Def(PrintOpNew(_, f, _)) => f
    case _                        => ???
  }
  override def printOp_Field_Limit[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[() => Boolean] = self match {
    case Def(PrintOpNew(_, _, limit)) => limit
    case _                            => ???
  }

  override def selectOp_Field_SelectPred[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A => Boolean] = self match {
    case Def(SelectOpNew(_, f)) => f
    case _                      => ???
  }

  // FIXME here it uses staging!
  override def loadLineitem(): Rep[Array[LINEITEMRecord]] = {
    val file = unit(Config.datapath + "lineitem.tbl")
    import scala.sys.process._;
    // val size = Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
    val size = fileLineCount(file)
    // Load Relation 
    val s = __newK2DBScanner(file)
    var i = __newVar[Int](0)
    val hm = __newArray[LINEITEMRecord](size)
    __whileDo(s.hasNext, {
      val newEntry = __newLINEITEMRecord(
        s.next_int,
        s.next_int,
        s.next_int,
        s.next_int,
        s.next_double,
        s.next_double,
        s.next_double,
        s.next_double,
        s.next_char,
        s.next_char,
        s.next_date,
        s.next_date,
        s.next_date,
        loadString(25, s),
        loadString(10, s),
        loadString(44, s))
      hm.update(i, newEntry)
      __assign(i, readVar(i) + unit(1))
      unit()
    })
    hm
  }

  def loadString(size: Rep[Int], s: Rep[K2DBScanner]) = {
    val NAME = __newArray[Byte](size)
    s.next(NAME)
    __newOptimalString(NAME.filter(__lambda(y => infix_!=(y, unit(0)))))
  }
}

trait LoopUnrolling extends pardis.ir.InlineFunctions { this: InliningLegoBase =>
  override def seqForeach[A, U](self: Rep[Seq[A]], f: Rep[A => U])(implicit manifestA: Manifest[A], manifestU: Manifest[U]): Rep[Unit] = self match {
    case Def(LiftedSeq(elems)) => elems.toList match {
      case Nil => unit(())
      case elem :: tail => {
        __app(f).apply(elem)
        __liftSeq(tail).foreach(f)
      }
    }
  }
}

