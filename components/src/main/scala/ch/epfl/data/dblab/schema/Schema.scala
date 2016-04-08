package ch.epfl.data
package dblab
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

case class Catalog(schemata: Map[String, Schema])
case class Schema(tables: ArrayBuffer[Table], stats: Statistics = Statistics()) {
  def findTable(name: String): Option[Table] = tables.find(t => t.name == name)
  def findTableByType(tpe: PardisType[_]): Option[Table] = tables.find(t => t.name + "Record" == tpe.name)
  def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.attributes).flatten.find(attr => attr.name == attrName)
}
case class Table(name: String, attributes: List[Attribute], constraints: ArrayBuffer[Constraint], resourceLocator: String, var rowCount: Long) {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }.toList
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }.toList
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }.toList
  def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
  def continuous: Option[Continuous] = constraints.collectFirst { case cont: Continuous => cont }
  def findAttribute(attrName: String): Option[Attribute] = attributes.find(attr => attr.name == attrName)
}
case class Attribute(name: String, dataType: Tpe, constraints: List[Constraint] = List(), var distinctValuesCount: Int = 0, var nullValuesCount: Long = 0) {
  def hasConstraint(con: Constraint) = constraints.contains(con)
}
object Attribute {
  implicit def tuple2ToAttribute(nameAndType: (String, Tpe)): Attribute = Attribute(nameAndType._1, nameAndType._2)
  implicit def tuple2oftuple2ToAttribute(nameAndTypeAndConstraints: ((String, Tpe), List[Constraint])): Attribute =
    Attribute(nameAndTypeAndConstraints._1._1, nameAndTypeAndConstraints._1._2, nameAndTypeAndConstraints._2)
}

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def foreignTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint
/**
 * Specifies that the rows of a given table are continues (which means it is
 * also a preimary key) with respect to the given attribute. The offset specifies
 * the offset between the index of a row and the value of the attribute.
 */
case class Continuous(attribute: Attribute, offset: Int) extends Constraint
object Compressed extends Constraint

