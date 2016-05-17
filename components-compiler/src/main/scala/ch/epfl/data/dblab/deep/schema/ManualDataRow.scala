package ch.epfl.data
package dblab
package deep
package schema

import java.io.PrintStream
import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.types._
import pardis.deep.scalalib.Tuple2IRs.Tuple2New

/** This deep class is manually created */
trait DynamicDataRowOps extends Base with TableOps {
  type DynamicDataRow = ch.epfl.data.dblab.schema.DynamicDataRow
  implicit val typeDynamicDataRow: TypeRep[DynamicDataRow] = DynamicDataRowIRs.DynamicDataRowType

  object DynamicDataRow {
    def apply(className: Rep[String])(values: Rep[(String, Any)]*): Rep[DynamicDataRow] = {
      val Constant(classNameString) = className
      val valuesConstant = values map { case Def(Tuple2New(Constant(_1: String), _2)) => (_1, false, _2) }
      val tp = //DynamicDataRowIRs.DynamicDataRowRecordType(classNameString)
        DynamicDataRowIRs.DynamicDataRowType
      record_new(valuesConstant)(tp).asInstanceOf[Rep[DynamicDataRow]]
    }
  }

  implicit class DynamicDataRowRep(drec: Rep[DynamicDataRow]) {
    def selectDynamic[T: TypeRep](key: Rep[String]): Rep[T] = key match {
      case Constant(name: String) => selectDynamic[T](name)
      case _                      => sys.error(s"field name for a dynamic record must be constant but was $key")
    }
    def selectDynamic[T: TypeRep](key: String): Rep[T] = record_select[DynamicDataRow, T](drec, key)
  }
}

object DynamicDataRowIRs extends Base {
  def DynamicDataRowRecordType(name: String): TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] = {
    new RecordType(StructTags.ClassTag(name), None)
  }
  val DynamicDataRowType: TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] =
    DynamicDataRowRecordType("DynamicDataRow")
  implicit val typeDynamicDataRow: TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] = DynamicDataRowType
  type DynamicDataRow = ch.epfl.data.dblab.schema.DynamicDataRow
}
