package ch.epfl.data
package dblab
package schema

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def foreignTable(implicit s: Schema): Option[Table] =
    s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] =
    s.tables.find(t => t.name == ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] =
    attributes.map {
      case (localAttr, foreignAttr) =>
        thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get
    }
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