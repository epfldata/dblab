package ch.epfl.data
package legobase
package shallow
package c
package collections

import pardis.annotations._
import pardis.shallow.c.CLang._
import pardis.shallow.c.CLangTypes._
import pardis.shallow.c.LGHashTableHeader._
import pardis.shallow.c.GLibTypes._

@metadeep(
  "legocompiler/src/main/scala/ch/epfl/data/legobase/deep",
  """
package ch.epfl.data
package legobase
package deep

import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.shallow.c.CLangTypes._
import pardis.shallow.c.GLibTypes._
import pardis.optimization._
import CTypes._
""",
  "",
  "DeepDSL")
class MetaInfo

@deep
@reflect[scala.collection.mutable.HashMap[_, _]]
@transformation
class CHashMap[K: CKeyType, V: CType](val gHashTable: LPointer[LGHashTable]) {
  def this() = {
    this(g_hash_table_new(&((s: gconstpointer) => {
      s.asInstanceOf[K].hashCode
    }), &((s1: gconstpointer, s2: gconstpointer) => {
      if (s1.asInstanceOf[K] == s2.asInstanceOf[K]) 1 else 0
    })))
  }

  def update(k: K, v: V) = {
    g_hash_table_insert(gHashTable, k.asInstanceOf[gconstpointer], v.asInstanceOf[gconstpointer])
  }

  def apply(k: K): V = {
    (g_hash_table_lookup(gHashTable, k.asInstanceOf[gconstpointer])).asInstanceOf[V]
  }

  def size: Int = g_hash_table_size(gHashTable)

  def clear(): Unit = {
    g_hash_table_remove_all(gHashTable)
  }

  def keySet = {
    //new CSet[K](g_hash_table_get_keys(gHashTable).asInstanceOf[LPointer[LGList[K]]])
    g_hash_table_get_keys(gHashTable).asInstanceOf[LPointer[LGList[K]]]
  }

  def contains(k: K): Boolean = {
    g_hash_table_lookup(gHashTable, k.asInstanceOf[gconstpointer]) != NULL[Any]
  }

  def remove(k: K) = {
    val v = apply(k)
    g_hash_table_remove(gHashTable, k.asInstanceOf[gconstpointer])
    //Some(v)
    v
  }

  def getOrElseUpdate(k: K, v: => V): V = {
    val res = g_hash_table_lookup(gHashTable, k.asInstanceOf[gconstpointer])
    if (res != NULL[V]) res.asInstanceOf[V]
    else {
      val vres = v
      //this(k) = v
      update(k, vres)
      vres
    }
  }
}
