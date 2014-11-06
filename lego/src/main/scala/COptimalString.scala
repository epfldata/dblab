package ch.epfl.data
package legobase
package shallow
package c

import pardis.annotations._
import pardis.shallow.c.CLang._
import pardis.shallow.c.CLangTypes._
import pardis.shallow.c.CString._
import pardis.shallow.c.CStdLib._

@metadeep(
  "legocompiler/src/main/scala/ch/epfl/data/legobase/deep",
  """
package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.shallow.c.CLangTypes
import pardis.optimization._
""",
  "",
  "DeepDSL")
class MetaInfo

@deep
@reflect[pardis.shallow.OptimalString]
@transformation
class COptimalString(val charArray: LPointer[Char] = NULL[Char]) {
  private implicit def getBaseValue(s: COptimalString) = s.charArray

  def string: LPointer[Char] = this

  def diff(y: COptimalString) = strcmp(this, y)

  // rewrite += rule {
  //   case OptimalStringDiff(oself, oy) =>
  //     val self = oself.asInstanceOf[LPointer[Char]]
  //     val y = oy.asInstanceOf[LPointer[Char]]
  //     dsl {
  //       strcmp(self, y)
  //     }
  // }

  def endsWith(y: COptimalString) = {
    val lenx = strlen(this)
    val leny = strlen(y)
    val len = lenx - leny
    strncmp(pointer_add(this, len), y, len) == 0
  }

  // rewrite += rule {
  //   case OptimalStringEndsWith(self, y) => ... dsl {
  //     val lenx = strlen(self)
  //     val leny = strlen(y)
  //     val len = lenx - leny
  //     strncmp(pointer_add(self, len), y, len) == 0
  //   }
  // }

  def startsWith(y: COptimalString) = {
    strncmp(this, y, strlen(y)) == 0
  }

  // rewrite += rule {
  //   case OptimalStringStartsWith(self, y) => ... dsl {
  //     strncmp(self, y, strlen(y)) == 0
  //   }
  // }

  def compare(y: COptimalString) = strcmp(this, y)

  // rewrite += rule {
  //   case OptimalStringCompare(self, y) => ... dsl {
  //     strcmp(self, y)
  //   }
  // }

  def length = strlen(this)

  // rewrite += rule {
  //   case OptimalStringLength(self) => ... dsl {
  //     strlen(self)
  //   }
  // }

  def ===(y: COptimalString) = strcmp(this, y) == 0

  def =!=(y: COptimalString) = strcmp(this, y) != 0

  def containsSlice(y: COptimalString) = strstr(this, y) != NULL[Char]

  def indexOfSlice(y: COptimalString, idx: Int) = {
    val substr = strstr(pointer_add(this, idx), y)
    if (substr == NULL[Char]) -1
    else str_subtract(substr, this)
  }

  def apply(idx: Int) = {
    *(pointer_add(this, idx))
  }

  def slice(start: Int, end: Int) = {
    val len = end - start + 1
    val newbuf = malloc[Char](len)
    strncpy(newbuf, pointer_add(this, start), len - 1)
    new COptimalString(newbuf)
  }
}

