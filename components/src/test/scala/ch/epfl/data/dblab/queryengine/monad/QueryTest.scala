package ch.epfl.data
package dblab
package queryengine
package monad

import org.scalatest._
import Matchers._
import sc.pardis.shallow.CaseClassRecord

case class A(i: Int, s: String) extends CaseClassRecord
case class B(j: Int, d: Double) extends CaseClassRecord

class QueryTest extends FlatSpec {
  val aList = List(A(1, "one"), A(2, "two"))
  val bList = List(B(1, 1.5), B(2, 2.5), B(3, 3.5))

  def mapSumQueryUnfold(al: List[A]): Int =
    QueryUnfold(al.toArray).map(_.i).sum

  def filterMapSumQueryUnfold(bl: List[B]): Int =
    QueryUnfold(bl.toArray).filter(_.d < 3).map(_.j).sum

  "map.sum unfold" should "work" in {
    mapSumQueryUnfold(aList) should be(3)
  }

  "filter.map.sum unfold" should "work" in {
    filterMapSumQueryUnfold(bList) should be(3)
  }

  def mapSumQueryStream(al: List[A]): Int =
    QueryStream(al.toArray).map(_.i).sum

  def filterMapSumQueryStream(bl: List[B]): Int =
    QueryStream(bl.toArray).filter(_.d < 3).map(_.j).sum

  "map.sum stream" should "work" in {
    mapSumQueryStream(aList) should be(3)
  }

  "filter.map.sum stream" should "work" in {
    filterMapSumQueryStream(bList) should be(3)
  }

  def mergeJoinQuery(al: List[A], bl: List[B]): Double =
    Query(al).mergeJoin(Query(bl))((x, y) => x.i - y.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  def mergeJoinQueryIterator(al: List[A], bl: List[B]): Double =
    QueryIterator(al.toArray).mergeJoin(QueryIterator(bl.toArray))((x, y) => x.i - y.j)((x, y) => x.i == y.j).map(_.d[Double]).sum
  // { QueryIterator(al.toArray).mergeJoin(QueryIterator(bl.toArray))((x, y) => x.i - y.j)((x, y) => x.i == y.j).foreach(println); println(">>>"); 3 }

  def mergeJoinQueryUnfold(al: List[A], bl: List[B]): Double =
    QueryUnfold(al.toArray).mergeJoin(QueryUnfold(bl.toArray))((x, y) => x.i - y.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  def mergeJoinQueryStream(al: List[A], bl: List[B]): Double =
    QueryStream(al.toArray).mergeJoin(QueryStream(bl.toArray))((x, y) => x.i - y.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  def hashJoinQuery(al: List[A], bl: List[B]): Double =
    Query(al).hashJoin(Query(bl))(_.i)(_.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  def hashJoinQueryIterator(al: List[A], bl: List[B]): Double =
    QueryIterator(al.toArray).hashJoin(QueryIterator(bl.toArray))(_.i)(_.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  def hashJoinQueryStream(al: List[A], bl: List[B]): Double =
    QueryStream(al.toArray).hashJoin(QueryStream(bl.toArray))(_.i)(_.j)((x, y) => x.i == y.j).map(_.d[Double]).sum

  "merge join" should "work in simple case" in {
    mergeJoinQuery(aList, bList) should be(4)
  }

  val b2List = List(B(1, 1.3), B(1, 1.7), B(2, -0.5), B(2, 1.5))
  "merge join" should "work in duplication case" in {
    mergeJoinQuery(aList, b2List) should be(4)
  }

  /* Merge-Join no longer works for N-M case!*/
  val a2List = List(A(1, "one-1"), A(1, "one-2"))
  // "merge join" should "work in 2 x duplication case" in {
  //   mergeJoinQuery(a2List, b2List) should be(6)
  // }

  "merge join iterator" should "work in simple case" in {
    mergeJoinQueryIterator(aList, bList) should be(4)
  }

  "merge join iterator" should "work in duplication case" in {
    mergeJoinQueryIterator(aList, b2List) should be(4)
  }

  "merge join unfold" should "work in simple case" in {
    mergeJoinQueryUnfold(aList, bList) should be(4)
  }

  "merge join unfold" should "work in duplication case" in {
    mergeJoinQueryUnfold(aList, b2List) should be(4)
  }

  "merge join stream" should "work in simple case" in {
    mergeJoinQueryStream(aList, bList) should be(4)
  }

  "merge join stream" should "work in duplication case" in {
    mergeJoinQueryStream(aList, b2List) should be(4)
  }

  /* Merge-Join no longer works for N-M case!*/
  // "merge join iterator" should "work in 2 x duplication case" in {
  //   mergeJoinQueryIterator(a2List, b2List) should be(6)
  // }

  val nonSortedList = List(A(1, "one"), A(3, "three"), A(2, "two-1"), A(2, "two-2"))

  "sort by iterator" should "work in distinct case" in {
    QueryIterator(nonSortedList.dropRight(1).toArray).sortBy(_.i).getList should be
    (List(A(1, "one"), A(2, "two-1"), A(3, "three")))
  }

  "sort by unfold" should "work in distinct case" in {
    QueryUnfold(nonSortedList.dropRight(1).toArray).sortBy(_.i).getList should be
    (List(A(1, "one"), A(2, "two-1"), A(3, "three")))
  }

  "sort by iterator" should "work in duplicated case" in {
    QueryIterator(nonSortedList.toArray).sortBy(_.i).getList should be
    (List(A(1, "one"), A(2, "two-1"), A(2, "two-2"), A(3, "three")))
  }

  "sort by unfold" should "work in duplicated case" in {
    QueryUnfold(nonSortedList.toArray).sortBy(_.i).getList should be
    (List(A(1, "one"), A(2, "two-1"), A(2, "two-2"), A(3, "three")))
  }

  "is sorted by" should "work for sorted list" in {
    Query(aList).isSortedBy(_.i) should be(true)
    Query(b2List).isSortedBy(x => (x.j, x.d)) should be(true)
  }

  "hash join" should "work in simple case" in {
    hashJoinQuery(aList, bList) should be(4)
  }

  "hash join iterator" should "work in simple case" in {
    hashJoinQueryIterator(aList, bList) should be(4)
  }

  "hash join stream" should "work in simple case" in {
    hashJoinQueryStream(aList, bList) should be(4)
  }

  val b3List = List(B(1, 1.3))

  "hash join iterator" should "work in a bit more complicated case" in {
    hashJoinQueryIterator(a2List, b3List) should be(2.6)
  }

  "hash join stream" should "work in a bit more complicated case" in {
    hashJoinQueryStream(a2List, b3List) should be(2.6)
  }
}
