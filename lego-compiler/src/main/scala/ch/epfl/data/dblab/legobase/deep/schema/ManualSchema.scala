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
trait SchemaOps extends Base with TableOps {
  type Schema = ch.epfl.data.dblab.legobase.schema.Schema
  implicit val typeSchema: TypeRep[Schema] = SchemaIRs.SchemaType

  val allTables = scala.collection.mutable.Set[Table]()

  // FIXME should not have Record postfix
  def getTable(tableName: String): Option[Table] = allTables.find(table => table.name + "Record" == tableName)
}

object SchemaIRs extends Base {
  case object SchemaType extends TypeRep[ch.epfl.data.dblab.legobase.schema.Schema] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = SchemaType
    val name = "Schema"
    val typeArguments = Nil
    val typeTag = scala.reflect.runtime.universe.typeTag[ch.epfl.data.dblab.legobase.schema.Schema]
  }
  implicit val typeSchema: TypeRep[ch.epfl.data.dblab.legobase.schema.Schema] = SchemaType
  type Schema = ch.epfl.data.dblab.legobase.schema.Schema
}

