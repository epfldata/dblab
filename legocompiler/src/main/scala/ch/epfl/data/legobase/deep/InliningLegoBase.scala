package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

trait InliningLegoBase extends DeepDSL with pardis.ir.InlineFunctions with LoopUnrolling {
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

  // FIXME needs apply virtualization
  override def operatorForeach[A](self: Rep[Operator[A]], f: Rep[A => Unit])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    reifyInline {
      var exit = __newVar(unit(false));
      __whileDo(`infix_!=`(exit, unit(true)), {
        val t = self.next();
        __ifThenElse(`infix_==`(t, self.NullDynamicRecord), __assign(exit, unit(true)), {
          __app(f).apply(t);
          unit(())
        })
      })
    }
  }

  // FIXME needs apply virtualization
  override def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[A => Boolean])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyInline {
      val exit = __newVar(unit(false));
      val res = __newVar(self.NullDynamicRecord);
      __whileDo(`infix_!=`(exit, unit(true)), {
        __assign(res, self.next());
        __ifThenElse(`infix_==`(res, self.NullDynamicRecord), __assign(exit, unit(true)), __assign(exit, __app(cond).apply(res)))
      });
      res
    }
  }

  override def printOpNext[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyInline {
      var exit = __newVar(unit(false));
      __whileDo(`infix_==`(exit, unit(false)), {
        val t = self.parent.next();
        __ifThenElse(`infix_==`(self.limit.apply(), unit(false)).||(`infix_==`(t, self.NullDynamicRecord)), __assign(exit, unit(true)), {
          self.printFunc.apply(t);
          self.`numRows_=`(self.numRows.+(unit(1)))
        })
      });
      self.NullDynamicRecord
    }
  }

  // FIXME hack handling ordering[A]
  override def sortOpNext[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = {
    implicit val ordering = new Ordering[A] {
      def compare(o1: A, o2: A) = ???
    }
    __ifThenElse(`infix_!=`(self.sortedTree.size, unit(0)), {
      val elem = self.sortedTree.head;
      self.sortedTree.-=(elem);
      elem
    }, self.NullDynamicRecord)
  }

  override def selectOpNext[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = reifyInline {
    self.parent.findFirst(self.selectPred)
  }

  override def scanOpNext[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = reifyInline {
    __ifThenElse(self.i.<(self.table.length), {
      val v = self.table.apply(self.i);
      self.`i_=`(self.i.+(unit(1)));
      v
    }, self.NullDynamicRecord)
  }

  // FIXME autolifter does not lift A to Rep[A]
  // FIXME needs apply virtualization
  override def mapOpNext[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyInline {
      val t = self.parent.next();
      __ifThenElse(`infix_!=`(t, self.NullDynamicRecord), {
        self.aggFuncs.foreach(__lambda((agg) => __app(agg).apply(t)));
        t
      }, self.NullDynamicRecord)
    }
  }

  override def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = reifyInline {
    __ifThenElse(`infix_!=`(self.hm.size, unit(0)), {
      val key = self.keySet.head;
      self.keySet.remove(key);
      val elem = self.hm.remove(key);
      newAGGRecord(key, elem.get)
    }, self.NullDynamicRecord)
  }

  override def printOpOpen[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    self.parent.open
  }

  override def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    unit(())
  }

  override def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    unit(())
  }

  // FIXME hack handling ordering[A]
  // FIXME autolifter does not lift A to Rep[A]
  override def sortOpOpen[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = reifyInline {
    implicit val ordering = new Ordering[A] {
      def compare(o1: A, o2: A) = ???
    }
    self.parent.open
    self.parent.foreach(__lambda((t: Rep[A]) => {
      self.sortedTree.+=(t);
      unit(())
    }))
  }

  override def mapOpOpen[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = reifyInline {
    self.parent.open
  }

  // // FIXME autolifter does not lift A to Rep[A]
  // // FIXME virtualize new array
  // // FIXME handle variables liftings
  // // FIXME autolift modules
  // // FIXME think more about varargs!
  // // FIXME remove : _* in YY transformation
  // FIXME needs apply virtualization
  // FIXME newArray hack related to issue #24
  override def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = {
    reifyInline {
      self.parent.open();
      self.parent.foreach(((t: Rep[A]) => {
        val key = self.grp.apply(t);
        val aggs = self.hm.getOrElseUpdate(key, __newArray[scala.Double](self.numAggs));
        var i: Var[Int] = __newVar(unit(0));
        self.aggFuncs.foreach(__lambda((aggFun) => {
          aggs.update(i, __app(aggFun).apply(t, aggs.apply(i)));
          __assign(i, readVar(i).+(unit(1)))
        }))
        unit(())
      }));
      // self.`keySet_=`(scala.collection.mutable.Set.apply(((self.hm.keySet.toSeq): _*)))
      self.`keySet_=`(Set.apply(self.hm.keySet.toSeq))
    }
  }

  // override def windowOpOpen[A, B, C](self: Rep[WindowOp[A, B, C]])(implicit manifestA: Manifest[A], manifestB: Manifest[B], manifestC: Manifest[C]): Rep[Unit] = {
  //   self.parent.open
  //   self.parent.foreach(__lambda { t: Rep[A] =>
  //     val key = self.grp(t)
  //     val v = self.hm.getOrElseUpdate(key, ArrayBuffer[A]())
  //     // v.append(t)
  //   })
  //   self.`keySet_=`(Set.apply(self.hm.keySet.toSeq))
  // }

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

  Config.datapath = "/Users/amirsh/Dropbox/yannis/"

  // FIXME here it uses staging!
  override def loadLineitem(): Rep[Array[LINEITEMRecord]] = {
    val file = Config.datapath + "lineitem.tbl"
    import scala.sys.process._;
    val size = Integer.parseInt((("wc -l " + file) #| "awk {print($1)}" !!).replaceAll("\\s+$", ""))
    // Load Relation 
    val s = __newK2DBScanner(unit(file))
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

