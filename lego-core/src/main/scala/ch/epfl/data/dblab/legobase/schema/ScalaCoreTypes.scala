package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types.PardisType
import scala.reflect.runtime.universe.{ TypeTag, typeTag => tag, Type }

case object DateType extends PardisType[java.util.Date] {
  def rebuild(newArguments: PardisType[_]*) = DateType
  val name = "java.util.Date"
  val typeArguments = Nil
  val typeTag = tag[java.util.Date]
}
