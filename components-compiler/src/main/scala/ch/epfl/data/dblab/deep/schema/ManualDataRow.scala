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
  implicit val typeDynamicDataRow: TypeRep[DynamicDataRow] = DynamicDataRowIRs.dynamicDataRowType

  def dataRowTypeForTable(table: Table): TypeRep[DynamicDataRow] = {
    DynamicDataRowIRs.dynamicDataRowRecordType(table.name)
  }
  def dataRowTypeForName(className: String): TypeRep[DynamicDataRow] = {
    DynamicDataRowIRs.dynamicDataRowRecordType(className)
  }

  object DynamicDataRow {
    def apply(className: Rep[String])(values: Rep[(String, Any)]*): Rep[DynamicDataRow] = {
      val Constant(classNameString) = className
      val valuesConstant = values map { case Def(Tuple2New(Constant(_1: String), _2)) => (_1, false, _2) }
      implicit val tp: TypeRep[DynamicDataRow] = DynamicDataRowIRs.dynamicDataRowRecordType(classNameString)
      // val tag = tp.asInstanceOf[RecordType[DynamicDataRow]].tag

      record_new(valuesConstant)(tp)
      // toAtom(Struct[DynamicDataRow](tag, createFieldsSyms(valuesConstant), Nil)(tp))(tp)
    }
  }

  implicit class DynamicDataRowRep(drec: Rep[DynamicDataRow]) {
    def selectDynamic[T: TypeRep](key: Rep[String]): Rep[T] = key match {
      case Constant(name: String) => selectDynamic[T](name)
      case _                      => sys.error(s"field name for a dynamic record must be constant but was $key")
    }
    def selectDynamic[T: TypeRep](key: String): Rep[T] = record_select[DynamicDataRow, T](drec, key)(drec.tp, implicitly[TypeRep[T]])
  }
}

object DynamicDataRowIRs extends Base {
  def dynamicDataRowRecordType(name: String): TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] = {
    DynamicDataRowRecordType(name).asInstanceOf[TypeRep[DynamicDataRow]]
  }
  case class DynamicDataRowRecordType(recordName: String) extends RecordType(StructTags.ClassTag("DDR_" + recordName), None)
  val dynamicDataRowType: TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] =
    dynamicDataRowRecordType("DynamicDataRow")
  implicit val typeDynamicDataRow: TypeRep[ch.epfl.data.dblab.schema.DynamicDataRow] = dynamicDataRowType
  type DynamicDataRow = ch.epfl.data.dblab.schema.DynamicDataRow
}
