import ch.epfl.data.legobase.utils.parser._
import org.scalatest._
import Matchers._

object Queries {
  val q1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-09-02' group by l_returnflzag, l_linestatus order by l_returnflag, l_linestatus"
}

class ParserSpec extends FlatSpec {

  "Parser" should "parse Q1 correctly" in {
    val parser = Parser
    val r = parser.parse(Queries.q1)
    r should not be None
  }
}
