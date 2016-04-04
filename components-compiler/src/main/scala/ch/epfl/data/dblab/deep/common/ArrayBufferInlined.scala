package ch.epfl.data
package dblab
package deep
package common

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep.scalalib.collection.ArrayBufferComponent

/** A polymorphic embedding cake containing some inlined methods for ArrayBuffer */
trait ArrayBufferInlined extends ArrayBufferComponent {
  // this one is needed to rewrire `ArrayBuffer.apply()` to `new ArrayBuffer()`
  // TODO Purgatory should handle that
  override def arrayBufferApplyObject[A](elems: Rep[A]*)(implicit typeA: TypeRep[A]): Rep[ArrayBuffer[A]] =
    if (elems.size == 0)
      __newArrayBuffer[A]()
    else
      this.arrayBufferApplyObject(elems: _*)
}
