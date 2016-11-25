package ch.epfl.data
package dblab
package schema

import sc.pardis.types.PardisType
import scala.reflect.runtime.universe.{ TypeTag, typeTag => tag, Type }

/**
 * Specifies the type of date attributes.
 */
case object DateType extends PardisType[java.util.Date] {
  def rebuild(newArguments: PardisType[_]*) = DateType
  val name = "java.util.Date"
  val typeArguments = Nil
  val typeTag = tag[java.util.Date]
}

/**
 * Specifies the type of fixed-length string attributes.
 *
 * The type contains the maximum length of string values.
 */
case class VarCharType(maxLength: Int) extends PardisType[String] {
  def rebuild(newArguments: PardisType[_]*) = VarCharType(maxLength)
  val name = "String"
  val typeArguments = Nil
  val typeTag = tag[String]
}

/**
 * Specifies the type of tables.
 *
 * The type contains the schema and table information.
 */
case class TableType(schema: Schema, table: Table) extends PardisType[Table] {
  def rebuild(newArguments: PardisType[_]*): PardisType[_] = new TableType(schema, table)
  val name = table.name
  val typeArguments = Nil
  override val isRecord = true
  val typeTag = throw new Exception("No type tag available for a TableType type representation.")
}

