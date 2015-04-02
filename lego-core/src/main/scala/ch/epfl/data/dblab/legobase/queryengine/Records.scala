package ch.epfl.data
package dblab.legobase
package queryengine

import sc.pardis.annotations.{ deep, needs }
import sc.pardis.shallow.{ CaseClassRecord, OptimalString }

@deep case class AGGRecord[B](
  val key: B,
  val aggs: Array[Double]) extends CaseClassRecord

@deep case class WindowRecord[B, C](
  val key: B,
  val wnd: C) extends CaseClassRecord
