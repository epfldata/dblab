package ch.epfl.data
package dblab
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import scala.collection.immutable.ListMap

case class Catalog(schemata: Map[String, Schema])
case class Schema(tables: List[Table], stats: Statistics = Statistics()) {
  def findTable(name: String): Option[Table] = tables.find(t => t.name == name)
  def findTableByType(tpe: PardisType[_]): Option[Table] = tables.find(t => t.name + "Record" == tpe.name)
  def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.attributes).flatten.find(attr => attr.name == attrName)
}
case class Table(name: String, attributes: List[Attribute], constraints: List[Constraint], resourceLocator: String, var rowCount: Long) {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }
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

// FIXME make the return types for the values consistent (i.e. all of them Long or Double!)
// TODO-GEN: Move this to its own file
case class Statistics() {
  private val QS_MEM_PREFIX = "QS_MEM_"
  private val CONFLICT_PREFIX = "CONFLICT_"
  private val CARDINALITY_PREFIX = "CARDINALITY_"
  private val DISTINCT_PREFIX = "DISTINCT_"
  private val statsMap = new scala.collection.mutable.HashMap[String, Double]()
  case class Dependency(name: String, func: Double => Double)
  private val statsDependencyMap = new scala.collection.mutable.HashMap[String, Dependency]()
  private def format(name: String) = name.replaceAll("Record", "").replaceAll("Type", "").
    replaceAll("\\(", "_").replaceAll("\\)", "").replaceAll("\\[", "_").replaceAll("\\]", "").replaceAll("\\ ", "").replaceAll("\\[", "").replaceAll(",", "_").toUpperCase()

  def +=(nameAndValue: (String, Double)) = statsMap += (format(nameAndValue._1) -> nameAndValue._2)
  def +=(name: String, value: Double) = statsMap += (format(name) -> value)
  def mkString(delim: String) = ListMap(statsMap.toSeq.sortBy(_._1): _*).mkString("\n========= STATISTICS =========\n", delim, "\n==============================\n")
  def increase(nameAndValue: (String, Double)) = statsMap += format(nameAndValue._1) -> (statsMap.getOrElse(format(nameAndValue._1), 0.0) + nameAndValue._2)
  def apply(statName: String): Double = statsMap(statName) // TODO-GEN: will die
  def addDependency(name1: String, name2: String, func: Double => Double): Unit = {
    statsDependencyMap += format(name1) -> Dependency(format(name2), func)
    // System.out.println(s"dep added from ${format(name1)} to ${format(name2)}")
  }

  // def getCardinalityOrElse(tableName: String, value: => Double): Double = statsMap.get(CARDINALITY_PREFIX + format(tableName)) match {
  //   case Some(stat) => stat
  //   case None       => value
  // }

  def getCardinalityOrElse(tableName: String, value: => Double): Double =
    getEstimatedNumObjectsForTypeOrElse(tableName, {
      statsMap.get(CARDINALITY_PREFIX + format(tableName)) match {
        case Some(stat) => stat
        case None       => value
      }

    })

  def getCardinality(tableName: String) = getCardinalityOrElse(tableName, {
    // This means that the statistics module has been asked for either a) a table that does not exist
    // in this schema or b) cardinality of an intermediate table that is being scanned over (see TPCH 
    // Q13 for an example about how this may happen). In both cases, we throw a warning message and
    // return the biggest cardinality
    System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics do not include cardinality information for table " + tableName + ". Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
    getLargestCardinality()
  })

  def getLargestCardinality() = {
    statsMap.foldLeft(1.0)((max, kv) => {
      if (kv._1.startsWith(CARDINALITY_PREFIX) && kv._2 > max) kv._2
      else max
    })
  }

  // TODO-GEN: The three following functions assume 1-N schemas. We have to make this explicit
  def getJoinOutputEstimation(tableNames: List[String]): Double = {
    val cardinalities = tableNames.map(getCardinality)
    val statKey = QS_MEM_PREFIX + tableNames.map(format).mkString("_")
    val value = cardinalities.max
    statsMap(statKey) = value
    value
  }

  def warningPerformance(key: String): Unit = {
    System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for $key not found.")
    System.out.println("Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
  }

  def getDistinctAttrValuesOrElse(attrName: String, value: => Int): Int = distinctAttributes(attrName) match {
    case Some(stat) => stat.toInt
    case None       => value
  }

  @deprecated("dangerous, use `distinctAttributes` instead", "")
  def getDistinctAttrValues(attrName: String): Int = getDistinctAttrValuesOrElse(attrName, {
    warningPerformance("In `getDistinctAttrValues` " + DISTINCT_PREFIX + attrName)
    getLargestCardinality().toInt // TODO-GEN: Make this return the cardinality of the corresponding table
  })

  def conflicts = new AttributeHandler(CONFLICT_PREFIX)
  def cardinalities = new AttributeHandler(CARDINALITY_PREFIX)
  def querySpecificCardinalities = new AttributeHandler(QS_MEM_PREFIX)
  def distinctAttributes = new AttributeHandler(DISTINCT_PREFIX)

  def getEstimatedNumObjectsForType(typeName: String): Double = getEstimatedNumObjectsForTypeOrElse(typeName, {
    warningPerformance(QS_MEM_PREFIX + format(typeName))
    getLargestCardinality()
  })

  def getEstimatedNumObjectsForTypeOrElse(typeName: String, value: => Double): Double = statsMap.get(QS_MEM_PREFIX + format(typeName)) match {
    case Some(v) => v
    case None => statsDependencyMap.get(format(typeName)) match {
      case Some(Dependency(name, func)) => func(getEstimatedNumObjectsForTypeOrElse(name, value))
      case None =>
        value
    }
  }

  def removeQuerySpecificStats() {
    // QS stands for Query specific
    statsMap.retain((k, v) => k.startsWith("QS") == false)
  }

  def getNumYearsAllDates(): Int = statsMap.get("NUM_YEARS_ALL_DATES") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for NUM_YEARS_ALL_DATES not found. Returning 128 years to compensate.")
      128
  }

  def getOutputSizeEstimation(): Int = statsMap.get("QS_OUTPUT_SIZE_ESTIMATION") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for QS_OUTPUT_SIZE_ESTIMATION not found. Returning largest cardinality to compensate.")
      getLargestCardinality().toInt // This is more than enough for all practical cases encountered so far
  }

  class AttributeHandler(prefix: String) {
    def update(attrName: String, value: Long): Unit = statsMap(prefix + format(attrName)) = value.toInt
    def +=(attrName: String, value: Long): Unit = apply(attrName) match {
      case Some(v) => update(attrName, v + value.toInt)
      case None    => update(attrName, value)
    }
    def apply(attrName: String): Option[Int] = statsMap.get(prefix + format(attrName)).map(_.toInt)
  }
}

// TODO add a type representation for fixed-size string