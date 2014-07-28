package ch.epfl.data
package legobase

import scala.language.implicitConversions

package object queryengine {
  implicit def toArrayByteOps(arr: Array[Byte]): ArrayByteOps = new ArrayByteOps(arr)
}
