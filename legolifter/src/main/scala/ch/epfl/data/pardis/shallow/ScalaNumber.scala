package scala

import ch.epfl.data._
import autolifter.annotations._

// TODO should be ported to pardis
@reflect[Int]
final abstract class MirrorInt {
  @pure def toByte: Byte
  @pure def toShort: Short
  @pure def toChar: Char
  @pure def toInt: Int
  @pure def toLong: Long
  @pure def toFloat: Float
  @pure def toDouble: Double
  @pure def unary_~ : Int
  @pure def unary_+ : Int
  @pure def unary_- : Int

  @pure def +(x: String): String
  @pure def <<(x: Int): Int
  @pure def <<(x: Long): Int
  @pure def >>>(x: Int): Int
  @pure def >>>(x: Long): Int
  @pure def >>(x: Int): Int
  @pure def >>(x: Long): Int

  @pure def ==(x: Byte): Boolean
  @pure def ==(x: Short): Boolean
  @pure def ==(x: Char): Boolean
  @pure def ==(x: Int): Boolean
  @pure def ==(x: Long): Boolean
  @pure def ==(x: Float): Boolean
  @pure def ==(x: Double): Boolean

  @pure def !=(x: Byte): Boolean
  @pure def !=(x: Short): Boolean
  @pure def !=(x: Char): Boolean
  @pure def !=(x: Int): Boolean
  @pure def !=(x: Long): Boolean
  @pure def !=(x: Float): Boolean
  @pure def !=(x: Double): Boolean

  @pure def <(x: Byte): Boolean
  @pure def <(x: Short): Boolean
  @pure def <(x: Char): Boolean
  @pure def <(x: Int): Boolean
  @pure def <(x: Long): Boolean
  @pure def <(x: Float): Boolean
  @pure def <(x: Double): Boolean

  @pure def <=(x: Byte): Boolean
  @pure def <=(x: Short): Boolean
  @pure def <=(x: Char): Boolean
  @pure def <=(x: Int): Boolean
  @pure def <=(x: Long): Boolean
  @pure def <=(x: Float): Boolean
  @pure def <=(x: Double): Boolean

  @pure def >(x: Byte): Boolean
  @pure def >(x: Short): Boolean
  @pure def >(x: Char): Boolean
  @pure def >(x: Int): Boolean
  @pure def >(x: Long): Boolean
  @pure def >(x: Float): Boolean
  @pure def >(x: Double): Boolean

  @pure def >=(x: Byte): Boolean
  @pure def >=(x: Short): Boolean
  @pure def >=(x: Char): Boolean
  @pure def >=(x: Int): Boolean
  @pure def >=(x: Long): Boolean
  @pure def >=(x: Float): Boolean
  @pure def >=(x: Double): Boolean

  @pure def |(x: Byte): Int
  @pure def |(x: Short): Int
  @pure def |(x: Char): Int
  @pure def |(x: Int): Int
  @pure def |(x: Long): Long

  @pure def &(x: Byte): Int
  @pure def &(x: Short): Int
  @pure def &(x: Char): Int
  @pure def &(x: Int): Int
  @pure def &(x: Long): Long

  @pure def ^(x: Byte): Int
  @pure def ^(x: Short): Int
  @pure def ^(x: Char): Int
  @pure def ^(x: Int): Int
  @pure def ^(x: Long): Long

  @pure def +(x: Byte): Int
  @pure def +(x: Short): Int
  @pure def +(x: Char): Int
  @pure def +(x: Int): Int
  @pure def +(x: Long): Long
  @pure def +(x: Float): Float
  @pure def +(x: Double): Double

  @pure def -(x: Byte): Int
  @pure def -(x: Short): Int
  @pure def -(x: Char): Int
  @pure def -(x: Int): Int
  @pure def -(x: Long): Long
  @pure def -(x: Float): Float
  @pure def -(x: Double): Double

  @pure def *(x: Byte): Int
  @pure def *(x: Short): Int
  @pure def *(x: Char): Int
  @pure def *(x: Int): Int
  @pure def *(x: Long): Long
  @pure def *(x: Float): Float
  @pure def *(x: Double): Double

  @pure def /(x: Byte): Int
  @pure def /(x: Short): Int
  @pure def /(x: Char): Int
  @pure def /(x: Int): Int
  @pure def /(x: Long): Long
  @pure def /(x: Float): Float
  @pure def /(x: Double): Double

  @pure def %(x: Byte): Int
  @pure def %(x: Short): Int
  @pure def %(x: Char): Int
  @pure def %(x: Int): Int
  @pure def %(x: Long): Long
  @pure def %(x: Float): Float
  @pure def %(x: Double): Double
}

@reflect[Double]
final abstract class MirrorDouble {
  @pure def toByte: Byte
  @pure def toShort: Short
  @pure def toChar: Char
  @pure def toInt: Int
  @pure def toLong: Long
  @pure def toFloat: Float
  @pure def toDouble: Double

  @pure def unary_+ : Double
  @pure def unary_- : Double
  @pure def +(x: String): String

  @pure def ==(x: Byte): Boolean
  @pure def ==(x: Short): Boolean
  @pure def ==(x: Char): Boolean
  @pure def ==(x: Int): Boolean
  @pure def ==(x: Long): Boolean
  @pure def ==(x: Float): Boolean
  @pure def ==(x: Double): Boolean

  @pure def !=(x: Byte): Boolean
  @pure def !=(x: Short): Boolean
  @pure def !=(x: Char): Boolean
  @pure def !=(x: Int): Boolean
  @pure def !=(x: Long): Boolean
  @pure def !=(x: Float): Boolean
  @pure def !=(x: Double): Boolean

  @pure def <(x: Byte): Boolean
  @pure def <(x: Short): Boolean
  @pure def <(x: Char): Boolean
  @pure def <(x: Int): Boolean
  @pure def <(x: Long): Boolean
  @pure def <(x: Float): Boolean
  @pure def <(x: Double): Boolean

  @pure def <=(x: Byte): Boolean
  @pure def <=(x: Short): Boolean
  @pure def <=(x: Char): Boolean
  @pure def <=(x: Int): Boolean
  @pure def <=(x: Long): Boolean
  @pure def <=(x: Float): Boolean
  @pure def <=(x: Double): Boolean

  @pure def >(x: Byte): Boolean
  @pure def >(x: Short): Boolean
  @pure def >(x: Char): Boolean
  @pure def >(x: Int): Boolean
  @pure def >(x: Long): Boolean
  @pure def >(x: Float): Boolean
  @pure def >(x: Double): Boolean

  @pure def >=(x: Byte): Boolean
  @pure def >=(x: Short): Boolean
  @pure def >=(x: Char): Boolean
  @pure def >=(x: Int): Boolean
  @pure def >=(x: Long): Boolean
  @pure def >=(x: Float): Boolean
  @pure def >=(x: Double): Boolean

  @pure def +(x: Byte): Double
  @pure def +(x: Short): Double
  @pure def +(x: Char): Double
  @pure def +(x: Int): Double
  @pure def +(x: Long): Double
  @pure def +(x: Float): Double
  @pure def +(x: Double): Double

  @pure def -(x: Byte): Double
  @pure def -(x: Short): Double
  @pure def -(x: Char): Double
  @pure def -(x: Int): Double
  @pure def -(x: Long): Double
  @pure def -(x: Float): Double
  @pure def -(x: Double): Double

  @pure def *(x: Byte): Double
  @pure def *(x: Short): Double
  @pure def *(x: Char): Double
  @pure def *(x: Int): Double
  @pure def *(x: Long): Double
  @pure def *(x: Float): Double
  @pure def *(x: Double): Double

  @pure def /(x: Byte): Double
  @pure def /(x: Short): Double
  @pure def /(x: Char): Double
  @pure def /(x: Int): Double
  @pure def /(x: Long): Double
  @pure def /(x: Float): Double
  @pure def /(x: Double): Double

  @pure def %(x: Byte): Double
  @pure def %(x: Short): Double
  @pure def %(x: Char): Double
  @pure def %(x: Int): Double
  @pure def %(x: Long): Double
  @pure def %(x: Float): Double
  @pure def %(x: Double): Double
}

@reflect[Long]
final abstract class MirrorLong {
  @pure def toByte: Byte
  @pure def toShort: Short
  @pure def toChar: Char
  @pure def toInt: Int
  @pure def toLong: Long
  @pure def toFloat: Float
  @pure def toDouble: Double

  @pure def unary_~ : Long
  @pure def unary_+ : Long
  @pure def unary_- : Long

  @pure def +(x: String): String

  @pure def <<(x: Int): Long
  @pure def <<(x: Long): Long
  @pure def >>>(x: Int): Long
  @pure def >>>(x: Long): Long
  @pure def >>(x: Int): Long
  @pure def >>(x: Long): Long

  @pure def ==(x: Byte): Boolean
  @pure def ==(x: Short): Boolean
  @pure def ==(x: Char): Boolean
  @pure def ==(x: Int): Boolean
  @pure def ==(x: Long): Boolean
  @pure def ==(x: Float): Boolean
  @pure def ==(x: Double): Boolean

  @pure def !=(x: Byte): Boolean
  @pure def !=(x: Short): Boolean
  @pure def !=(x: Char): Boolean
  @pure def !=(x: Int): Boolean
  @pure def !=(x: Long): Boolean
  @pure def !=(x: Float): Boolean
  @pure def !=(x: Double): Boolean

  @pure def <(x: Byte): Boolean
  @pure def <(x: Short): Boolean
  @pure def <(x: Char): Boolean
  @pure def <(x: Int): Boolean
  @pure def <(x: Long): Boolean
  @pure def <(x: Float): Boolean
  @pure def <(x: Double): Boolean

  @pure def <=(x: Byte): Boolean
  @pure def <=(x: Short): Boolean
  @pure def <=(x: Char): Boolean
  @pure def <=(x: Int): Boolean
  @pure def <=(x: Long): Boolean
  @pure def <=(x: Float): Boolean
  @pure def <=(x: Double): Boolean

  @pure def >(x: Byte): Boolean
  @pure def >(x: Short): Boolean
  @pure def >(x: Char): Boolean
  @pure def >(x: Int): Boolean
  @pure def >(x: Long): Boolean
  @pure def >(x: Float): Boolean
  @pure def >(x: Double): Boolean

  @pure def >=(x: Byte): Boolean
  @pure def >=(x: Short): Boolean
  @pure def >=(x: Char): Boolean
  @pure def >=(x: Int): Boolean
  @pure def >=(x: Long): Boolean
  @pure def >=(x: Float): Boolean
  @pure def >=(x: Double): Boolean

  @pure def |(x: Byte): Long
  @pure def |(x: Short): Long
  @pure def |(x: Char): Long
  @pure def |(x: Int): Long
  @pure def |(x: Long): Long

  @pure def &(x: Byte): Long
  @pure def &(x: Short): Long
  @pure def &(x: Char): Long
  @pure def &(x: Int): Long
  @pure def &(x: Long): Long

  @pure def ^(x: Byte): Long
  @pure def ^(x: Short): Long
  @pure def ^(x: Char): Long
  @pure def ^(x: Int): Long
  @pure def ^(x: Long): Long

  @pure def +(x: Byte): Long
  @pure def +(x: Short): Long
  @pure def +(x: Char): Long
  @pure def +(x: Int): Long
  @pure def +(x: Long): Long
  @pure def +(x: Float): Float
  @pure def +(x: Double): Double

  @pure def -(x: Byte): Long
  @pure def -(x: Short): Long
  @pure def -(x: Char): Long
  @pure def -(x: Int): Long
  @pure def -(x: Long): Long
  @pure def -(x: Float): Float
  @pure def -(x: Double): Double

  @pure def *(x: Byte): Long
  @pure def *(x: Short): Long
  @pure def *(x: Char): Long
  @pure def *(x: Int): Long
  @pure def *(x: Long): Long
  @pure def *(x: Float): Float
  @pure def *(x: Double): Double

  @pure def /(x: Byte): Long
  @pure def /(x: Short): Long
  @pure def /(x: Char): Long
  @pure def /(x: Int): Long
  @pure def /(x: Long): Long
  @pure def /(x: Float): Float
  @pure def /(x: Double): Double

  @pure def %(x: Byte): Long
  @pure def %(x: Short): Long
  @pure def %(x: Char): Long
  @pure def %(x: Int): Long
  @pure def %(x: Long): Long
  @pure def %(x: Float): Float
  @pure def %(x: Double): Double
}

// TODO should be ported to pardis
@reflect[Boolean]
final abstract class MirrorBoolean {
  @pure def unary_! : Boolean
  @pure def ==(x: Boolean): Boolean
  @pure def !=(x: Boolean): Boolean
  @pure def ||(x: => Boolean): Boolean
  @pure def &&(x: => Boolean): Boolean

  @pure def |(x: Boolean): Boolean
  @pure def &(x: Boolean): Boolean
  @pure def ^(x: Boolean): Boolean
}

@reflect[java.lang.Integer]
abstract class MirrorInteger(val value: Int) {
  override def equals(x$1: Any): Boolean = ???
  def compareTo(x$1: Integer): Int = ???
  def byteValue(): Byte = ???
  def shortValue(): Short = ???
  def intValue(): Int = ???
  def longValue(): Long = ???
  def floatValue(): Float = ???
  def doubleValue(): Double = ???
}

@reflect[java.lang.Character]
abstract class MirrorCharacter(val value: Char) {
  override def equals(x$1: Any): Boolean = ???
  def compareTo(x$1: java.lang.Character): Int = ???
  def charValue(): Char = ???
}