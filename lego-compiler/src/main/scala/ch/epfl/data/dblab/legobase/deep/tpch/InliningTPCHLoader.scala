package ch.epfl.data
package dblab.legobase
package deep
package tpch

import scala.language.implicitConversions
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import dblab.legobase.schema._

/** A polymorphic embedding cake for manually inlining some methods of [[ch.epfl.data.dblab.legobase.tpch.TPCHLoader]] */
trait InliningTPCHLoader extends tpch.TPCHLoaderImplementations { this: InliningLegoBase =>
  override def tPCHLoaderGetTableObject(tableName: Rep[String]): Rep[Table] = tableName match {
    case Constant(v) => unit(dblab.legobase.tpch.TPCHLoader.getTable(v))
  }
}
