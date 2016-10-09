package ch.epfl.data
package dblab
package frontend
package parser

/**
 *
 * @author Yannis Klonatos
 */
object DDLAST {
  // DDL Attribute Types 
  sealed trait AttributeType
  final case class DDLInteger() extends AttributeType
  final case class DDLChar(numChars: Int) extends AttributeType
  final case class DDLVarChar(numChars: Int) extends AttributeType
  final case class DDLDecimal(integerDigits: Int, decimalPoints: Int) extends AttributeType
  final case class DDLDate() extends AttributeType

  final case class DDLTable(name: String, columns: List[Column], constraints: Set[ConstraintOp])
  final case class Column(name: String, datatype: AttributeType, notNull: Boolean,
                          autoInc: Boolean, defaultVal: Option[String], annotations: List[String])
  sealed trait Constraint
  final case class DDLUniqueKey(table: String, uniqueCols: List[String]) extends Constraint
  final case class DDLPrimaryKey(table: String, primaryKeyCols: List[String]) extends Constraint
  final case class DDLForeignKey(table: String, foreignKeyName: String, foreignKeyCols: List[String],
                                 foreignTable: String) extends Constraint

  trait CatalogOp
  final case class UseSchema(sName: String) extends CatalogOp
  final case class ConstraintOp(add: Boolean, cons: Constraint) extends CatalogOp
  final case class DropTable(tableName: String) extends CatalogOp
}