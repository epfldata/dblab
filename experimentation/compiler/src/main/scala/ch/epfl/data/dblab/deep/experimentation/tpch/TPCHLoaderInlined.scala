package ch.epfl.data
package dblab
package deep
package experimentation
package tpch

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import dblab.schema._
import storagemanager._
import scala.reflect.runtime.universe.{ typeOf, Type }

/** A polymorphic embedding cake for manually inlining some methods of [[ch.epfl.data.dblab.tpch.TPCHLoader]] */
trait TPCHLoaderInlined extends TPCHLoaderImplementations with LoaderInlined {
  override def tPCHLoaderGetTableObject(tableName: Rep[String]): Rep[Table] = tableName match {
    case Constant(v) => unit(dblab.experimentation.tpch.TPCHLoader.getTable(v))
  }
  override def componentType: Type = typeOf[TPCHLoaderInlined]
}
