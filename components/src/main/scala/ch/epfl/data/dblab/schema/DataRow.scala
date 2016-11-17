package ch.epfl.data
package dblab
package schema

import sc.pardis.shallow.{ Record, OptimalString }

/**
 * Represents a row in a database relation.
 *
 * @author Yannis Klonatos
 */
class DataRow(val values: Seq[(String, Any)]) extends Record {
  private val fieldMap = collection.mutable.HashMap[String, Any](values: _*)
  def numFields = fieldMap.size
  def getField(name: String): Option[Any] = fieldMap.get(name) match {
    case Some(fld) => Some(fld)
    case None =>
      var stop = false
      var res: Option[Any] = None
      for (f <- getNestedRecords() if !stop) {
        f.getField(name) match {
          case Some(r) => res = Some(r); stop = true;
          case None    =>
        }
      }
      res
  }
  def setField(name: String, value: Any) {
    if (!fieldMap.keySet.contains(name))
      throw new Exception("Cannot insert new field " + name + " in DataRow after its creation (existing fields are " + fieldMap.keySet + ")")
    fieldMap += name -> value
  }
  def getFieldNames() = fieldMap.keySet
  def getNestedRecords(): Seq[DataRow] = fieldMap.map({ case (k, v) => v }).filter(_.isInstanceOf[DataRow]).toSeq.asInstanceOf[Seq[DataRow]]
  override def toString = "DataRow(" + fieldMap.toSeq.toString + ")"
  override def hashCode: Int = values.map(_._2.hashCode).sum
  override def equals(o: Any): Boolean = o match {
    case dr: DataRow => values.zip(dr.values).forall(x => x._1._1 == x._2._1 && x._1._2 == x._2._2)
    case _           => false
  }
}

import scala.language.dynamics

class DynamicDataRow(val className: String, override val values: Seq[(String, Any)]) extends DataRow(values) with Dynamic {
  def selectDynamic[T](key: String): T = {
    getField(key).getOrElse(sys.error(s"$this does not have $key field")).asInstanceOf[T]
  }
}

object DynamicDataRow {
  def apply(className: String)(values: (String, Any)*): DynamicDataRow =
    new DynamicDataRow(className, values.toSeq)
}
