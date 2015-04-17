package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions

class Catalog(schemata: Map[String, Schema])
class Schema(tables: List[Table])
class Table(attributes: List[Attribute], constraints: List[Constraint], resourceLocator: String, rowCount: Long, estimatedMemorySizeInBytes: Long)
// TODO what's the default value for distinctValuesCount and nullValuesCount
class Attribute(name: String, dataType: Tpe, distinctValuesCount: Int, nullValuesCount: Long)
object Attribute {
  implicit def tuple2ToAttribute(nameAndType: (String, Tpe)): Attribute = new Attribute(nameAndType._1, nameAndType._2, 0, 0)
}

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
// TODO chicken-egg problem
case class ForeignKey(referencedTable: Table, attributes: List[(Attribute, Attribute)], selectivity: Double) extends Constraint
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint

// TODO add a type representation for fixed-size string
