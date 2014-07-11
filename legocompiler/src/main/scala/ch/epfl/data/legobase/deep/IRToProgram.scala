package ch.epfl.data
package legobase
package deep

import scala.language.implicitConversions
import pardis.ir._
import reflect.runtime.universe.{ TypeTag, Type }

trait IRToProgram extends TopDownTransformer[LoweringLegoBase, LoweringLegoBase] with Traverser[LoweringLegoBase] {
  import from._

  val from = IR
  val to = IR

  def createProgram(node: Block[_]): PardisProgram = {
    traverseBlock(node)
    val structsDef = structs.toList.map(x => PardisStructDef(x._1, x._2))
    PardisProgram(structsDef, node)
  }

  val structs = collection.mutable.HashMap.empty[StructTags.StructTag[_], Seq[(String, Type)]]

  override def traverseDef(node: Def[_]): Unit = node match {
    case Struct(tag, elems) => {
      def getTypeTag(m: Manifest[Any]): TypeTag[Any] = reflect.runtime.universe.internal.manifestToTypeTag[Any](scala.reflect.runtime.currentMirror, m).asInstanceOf[TypeTag[Any]]
      def getType(m: Manifest[Any]): Type = getTypeTag(m).tpe
      structs += tag -> elems.map(x => x._1 -> getType(x._2.tp))
    }
    case _ => super.traverseDef(node)
  }

  override def transformDef[T: Manifest](node: Def[T]): to.Def[T] = node match {
    case an: AggOpNew[_, _] => {
      val ma = an.manifestA
      val mb = an.manifestB
      val marrDouble = manifest[Array[Double]]
      to.reifyBlock({
        to.__new[T](("hm", false, to.__newHashMap(to.overloaded2, mb, marrDouble)))
      }).correspondingNode
    }
    case _ => super.transformDef(node)
  }
}
