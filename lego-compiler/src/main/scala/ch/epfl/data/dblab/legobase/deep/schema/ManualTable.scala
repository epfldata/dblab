package ch.epfl.data
package dblab.legobase
package deep
package schema

import java.io.PrintStream
import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._

/** This deep class is manually created */
trait TableOps extends Base {
  type Table = ch.epfl.data.dblab.legobase.schema.Table
  implicit val typeTable: TypeRep[Table] = TableIRs.TableType
}

object TableIRs extends Base {
  case object TableType extends TypeRep[ch.epfl.data.dblab.legobase.schema.Table] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = TableType
    val name = "Table"
    val typeArguments = Nil
    val typeTag = scala.reflect.runtime.universe.typeTag[ch.epfl.data.dblab.legobase.schema.Table]
  }
  implicit val typeTable: TypeRep[ch.epfl.data.dblab.legobase.schema.Table] = TableType
  type Table = ch.epfl.data.dblab.legobase.schema.Table
}

