package scala

import ch.epfl.data._
import autolifter.annotations._

@reflect[Array[_]]
final class MirrorArray[T](_length: Int) extends java.lang.Cloneable {
  @read def length: Int = ???
  @read def apply(i: Int): T = ???
  @write def update(i: Int, x: T): Unit = ???
  def filter(p: T => Boolean): Array[T] = ???
  override def clone(): Array[T] = ???
}
