package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

trait InliningLegoBase extends DeepDSL with pardis.ir.InlineFunctions {
  // override def lambdaApply[T: Manifest, S: Manifest](fun: Rep[T => S], input: Rep[T]): Rep[S] = fun match {
  //   case Def(Lambda(i, Block(stmts, res))) => Block(Stm(i.asInstanceOf[Sym[T]], ReadVal(input)) :: stmts, res)
  //   case _                                 => super.lambdaApply(fun, input)
  // }

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
    reifyBlock {
      var exit = __newVar(unit(false));
      __whileDo(`infix_!=`(exit, unit(true)), {
        val t = self.next();
        __ifThenElse(`infix_==`(t, self.NullDynamicRecord), __assign(exit, unit(true)), {
          new LambdaRep(f).apply(t);
          unit(())
        })
      })
    }
  }

  // FIXME needs apply virtualization
  override def operatorFindFirst[A](self: Rep[Operator[A]], cond: Rep[A => Boolean])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyBlock {
      val exit = __newVar(unit(false));
      val res = __newVar(self.NullDynamicRecord);
      __whileDo(`infix_!=`(exit, unit(true)), {
        __assign(res, self.next());
        __ifThenElse(`infix_==`(res, self.NullDynamicRecord), __assign(exit, unit(true)), __assign(exit, new LambdaRep(cond).apply(res)))
      });
      res
    }
  }

  override def printOpNext[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyBlock {
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

  override def selectOpNext[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = reifyBlock {
    self.parent.findFirst(self.selectPred)
  }

  override def scanOpNext[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = reifyBlock {
    __ifThenElse(self.i.<(self.table.length), {
      val v = self.table.apply(self.i);
      self.`i_=`(self.i.+(unit(1)));
      v
    }, self.NullDynamicRecord)
  }

  // FIXME autolifter does not lift A to Rep[A]
  // FIXME needs apply virtualization
  override def mapOpNext[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[A] = {
    reifyBlock {
      val t = self.parent.next();
      __ifThenElse(`infix_!=`(t, self.NullDynamicRecord), {
        self.aggFuncs.foreach(__lambda((agg) => new LambdaRep(agg).apply(t)));
        t
      }, self.NullDynamicRecord)
    }
  }

  override def aggOpNext[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[AGGRecord[B]] = reifyBlock {
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
  override def sortOpOpen[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = reifyBlock {
    implicit val ordering = new Ordering[A] {
      def compare(o1: A, o2: A) = ???
    }
    self.parent.open
    self.parent.foreach(__lambda((t: Rep[A]) => {
      self.sortedTree.+=(t);
      unit(())
    }))
  }

  override def mapOpOpen[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = reifyBlock {
    self.parent.open
  }

  // // class Lambda2Rep2_2[A1: Manifest, A2: Manifest, B: Manifest](f: Rep[(A1, A2) => B]) {
  // //   def apply(x1: Rep[A1], x2: Rep[A2]): Rep[B] = lambdaApply[(A1, A2), B](f.asInstanceOf[Rep[((A1, A2)) => B]], tupled2(x1, x2))
  // // }

  def toLambda22[A1: Manifest, A2: Manifest, B: Manifest](x: Rep[(A1, A2) => B]): Lambda2Rep2[A1, A2, B] = new Lambda2Rep2(x)

  // // FIXME autolifter does not lift A to Rep[A]
  // // FIXME virtualize new array
  // // FIXME handle variables liftings
  // // FIXME autolift modules
  // // FIXME think more about varargs!
  // // FIXME remove : _* in YY transformation
  // FIXME needs apply virtualization
  override def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = {
    reifyBlock {
      self.parent.open();
      self.parent.foreach(((t: Rep[A]) => {
        val key = self.grp.apply(t);
        val aggs = self.hm.getOrElseUpdate(key, __newArray[scala.Double](self.numAggs));
        var i: Var[Int] = __newVar(unit(0));
        self.aggFuncs.foreach(__lambda((aggFun) => {
          aggs.update(i, toLambda22(aggFun).apply(t, aggs.apply(i)));
          __assign(i, readVar(i).+(unit(1)))
        }))
        unit(())
      }));
      // self.`keySet_=`(scala.collection.mutable.Set.apply(((self.hm.keySet.toSeq): _*)))
      self.`keySet_=`(Set.apply(self.hm.keySet.toSeq))
    }
  }

  override def printOp_Field_parent[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: PrintOpNew[_]) => x.parent
      case _                     => super.printOp_Field_parent(self)
    }
  }

  override def sortOp_Field_parent[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SortOpNew[_]) => x.parent
      case _                    => super.sortOp_Field_parent(self)
    }
  }

  override def mapOp_Field_parent[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: MapOpNew[_]) => x.parent
      case _                   => super.mapOp_Field_parent(self)
    }
  }

  override def selectOp_Field_parent[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Operator[A]] = {
    self match {
      case Def(x: SelectOpNew[_]) => x.parent
      case _                      => super.selectOp_Field_parent(self)
    }
  }

  override def aggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = {
    self match {
      case Def(x: AggOpNew[_, _]) => x.parent
      case _                      => super.aggOp_Field_parent(self)
    }
  }
}
