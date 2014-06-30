package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions

trait InliningLegoBase extends DeepDSL {
  override def operatorOpen[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = self match {
    case Def(x: PrintOpNew[_])  => self.asInstanceOf[Rep[PrintOp[A]]].open
    case Def(x: SortOpNew[_])   => self.asInstanceOf[Rep[SortOp[A]]].open
    case Def(x: MapOpNew[_])    => self.asInstanceOf[Rep[MapOp[A]]].open
    case Def(x: AggOpNew[_, _]) => self.asInstanceOf[Rep[AggOp[A, Any]]].open
    case Def(x: ScanOpNew[_])   => self.asInstanceOf[Rep[ScanOp[A]]].open
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].open
    case _                      => super.operatorOpen(self)
  }

  override def operatorNext[A](self: Rep[Operator[A]])(implicit manifestA: Manifest[A]): Rep[A] = self match {
    case Def(x: PrintOpNew[_])  => self.asInstanceOf[Rep[PrintOp[A]]].next
    case Def(x: SortOpNew[_])   => self.asInstanceOf[Rep[SortOp[A]]].next
    case Def(x: MapOpNew[_])    => self.asInstanceOf[Rep[MapOp[A]]].next
    case Def(x: AggOpNew[_, _]) => self.asInstanceOf[Rep[AggOp[A, Any]]].next.asInstanceOf[Rep[A]]
    case Def(x: ScanOpNew[_])   => self.asInstanceOf[Rep[ScanOp[A]]].next
    case Def(x: SelectOpNew[_]) => self.asInstanceOf[Rep[SelectOp[A]]].next
    case _                      => super.operatorNext(self)
  }
  // seems not a good idea
  // manifestA.erasure match {
  //   case x if x == manifest[PrintOp[Any]].erasure => printOpOpen(self.asInstanceOf[Rep[PrintOp[A]]])
  // }

  override def printOpOpen[A](self: Rep[PrintOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    self.parent.open
  }

  override def scanOpOpen[A](self: Rep[ScanOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    unit(())
  }

  override def selectOpOpen[A](self: Rep[SelectOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    unit(())
  }

  override def sortOpOpen[A](self: Rep[SortOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = {
    self.parent.open
  }

  override def mapOpOpen[A](self: Rep[MapOp[A]])(implicit manifestA: Manifest[A]): Rep[Unit] = reifyBlock {
    self.parent.open
    self.parent.foreach { t: Rep[A] =>
      // FIXME add the real implementation
      unit(())
    }
  }

  override def aggOpOpen[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Unit] = reifyBlock {
    self.parent.open
    self.parent.foreach { t: Rep[A] =>
      // FIXME add the real implementation
      unit(())
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

  override def aggOp_Field_parent[A, B](self: Rep[AggOp[A, B]])(implicit manifestA: Manifest[A], manifestB: Manifest[B]): Rep[Operator[A]] = {
    self match {
      case Def(x: AggOpNew[_, _]) => x.parent
      case _                      => super.aggOp_Field_parent(self)
    }
  }
}
