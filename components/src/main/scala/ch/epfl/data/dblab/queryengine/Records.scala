package ch.epfl.data
package dblab
package queryengine

import sc.pardis.annotations.{ deep, needs }
import sc.pardis.shallow.{ CaseClassRecord, OptimalString }

/**
 * A record which is used in Aggregate Operators
 */
@deep case class AGGRecord[B](
  val key: B,
  val aggs: Array[Double]) extends CaseClassRecord

/**
 * A record which is used in Window Operators
 */
@deep case class WindowRecord[B, C](
  val key: B,
  val wnd: C) extends CaseClassRecord
