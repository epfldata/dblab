package ch.epfl.data
package legobase
package schema

import scala.reflect.api.Types

class Catalog(schemata: Map[String, Schema])
class Schema(tables: List[Table])
class Table(attributes: Map[String, Attribute], constraints: List[Constraint], resourceLocator: String, rowCount: Long, estimatedMemorySizeInBytes: Long)
class Attribute(name: String, /*dataType: ??, */ distinctValuesCount: Int, nullValuesCount: Long)

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
case class ForeignKey(referencedTable: Table, attributes: List[(Attribute, Attribute)], selectivity: Double) extends Constraint
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint