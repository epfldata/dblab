package ch.epfl.data
package dblab
package transformers
package monad

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import deep.dsls.QueryEngineExp
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

trait StructProcessing[Lang <: Base] extends StructCollector[Lang] {
  import IR._

  val recordUsageAnalysis: RecordUsageAnalysis[Lang]

  def getStructDefByTag[T: TypeRep](tag: StructTags.StructTag[T]): Option[PardisStructDef[T]] =
    structsDefMap.get(tag).asInstanceOf[Option[PardisStructDef[T]]]

  def getFieldsByTag[T: TypeRep](tag: StructTags.StructTag[T]): Seq[StructElemInformation] = tag match {
    case StructTags.CompositeTag(_, _, lt, rt) => getFieldsByTag(lt) ++ getFieldsByTag(rt)
    case _ => getStructDefByTag(tag) match {
      case Some(structDef) => structDef.fields
      case None            => throw new Exception(s"getStructDefByTag not found for tag $tag")
    }
  }

  def getFields[T: TypeRep]: Seq[StructElemInformation] = typeRep[T] match {
    case rt: RecordType[T] => getFieldsByTag(rt.tag)
    case t                 => throw new Exception(s"No fields exist for ${typeRep[T]}")
  }

  def getRegisteredFieldsOfType[A: TypeRep]: List[String] = {
    typeRep[A] match {
      case rt: RecordType[_] if rt.originalType.nonEmpty => getRegisteredFieldsOfType(rt.originalType.get)
      case t => recordUsageAnalysis.getRegisteredFieldsOfType(t)
    }
  }

  def concat_records[T: TypeRep, S: TypeRep, Res: TypeRep](elem1: Rep[T], elem2: Rep[S]): Rep[Res] = {
    val resultType = typeRep[Res]
    val regFields = getRegisteredFieldsOfType(elem1.tp) ++ getRegisteredFieldsOfType(elem2.tp)
    // System.out.println(s"regFields: $regFields")
    def fieldIsRegistered(f: StructElemInformation): Boolean = regFields.contains(f.name) || !recordUsageAnalysis.removeUnusedFields
    val elems1 = getFields[T].filter(fieldIsRegistered).map(x => PardisStructArg(x.name, x.mutable, field(elem1, x.name)(x.tpe)))
    val elems2 = getFields[S].filter(fieldIsRegistered).map(x => PardisStructArg(x.name, x.mutable, field(elem2, x.name)(x.tpe)))
    val structFields = elems1 ++ elems2
    struct(structFields: _*)(resultType)
  }
}