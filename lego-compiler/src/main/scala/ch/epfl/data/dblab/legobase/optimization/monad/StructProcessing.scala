package ch.epfl.data
package dblab.legobase
package optimization
package monad

import schema._
import scala.language.implicitConversions
import sc.pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }
import sc.pardis.optimization._
import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

trait StructProcessing[Lang <: Base] extends StructCollector[Lang] {
  import IR._
  def concat_types[T: TypeRep, S: TypeRep, Res: TypeRep]: TypeRep[Res] = {
    // val leftTag = typeRep[T].asInstanceOf[RecordType[T]].tag
    // val rightTag = typeRep[S].asInstanceOf[RecordType[S]].tag
    // val concatTag = StructTags.CompositeTag[T, S]("", "", leftTag, rightTag)
    // new RecordType(concatTag, Some(typeRep[Res].asInstanceOf[TypeRep[Any]])).asInstanceOf[TypeRep[Res]]
    typeRep[Res]
  }

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
  //   typeRep[T] match {
  //     case rt: RecordType[T] => getStructDef(rt) match {
  //       case Some(d) => d.fields
  //       case _ => x.originalType.get match {
  //         case Dyna
  //       }
  //     }
  //   }
  //  getStructDef(typeRep[T]) match {
  //   case Some(d) => d.fields
  //   case _ =>
  //     typeRep[T] match {
  //       case x: RecordType[T] => x.originalType.get match {
  //         case DynamicCompositeRecordType(lt, rt) => getFields(lt) ++ getFields(rt)
  //       }
  //     }
  //     throw new Exception(s"No fields exist for ${typeRep[T]}")
  // }

  def concat_records[T: TypeRep, S: TypeRep, Res: TypeRep](elem1: Rep[T], elem2: Rep[S]): Rep[Res] = {
    val resultType = concat_types[T, S, Res]
    val elems1 = getFields[T].map(x => PardisStructArg(x.name, x.mutable, field(elem1, x.name)(x.tpe)))
    val elems2 = getFields[S].map(x => PardisStructArg(x.name, x.mutable, field(elem2, x.name)(x.tpe)))
    // val structFields = elems.zip(elemsRhs).map(x => PardisStructArg(x._1.name, x._1.mutable, field(x._2.rec, x._2.name)(x._2.tp)))
    val structFields = elems1 ++ elems2
    struct(structFields: _*)(resultType)
  }
}