import ch.epfl.data.legobase.utils.parser._
import org.scalatest._
import Matchers._

object Queries {
  val q1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-09-02' group by l_returnflzag, l_linestatus order by l_returnflag, l_linestatus"
  val q1upper = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflzag, l_linestatus ORDER BY l_returnflag, l_linestatus"
}

class ParserSpec extends FlatSpec {

  "Parser" should "parse Q1 correctly" in {
    val parser = Parser
    val r = parser.parse(Queries.q1)
    r should not be None
  }

  "Parser" should "parse Q1 with keywords uppercase correctly" in {
    val parser = Parser
    val r = parser.parse(Queries.q1upper)
    r should not be None
  }

  "Parser" should "parse simple SELECT/FROM correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT * FROM table t")
    r should not be None
  }

  "Parser" should "parse simple SELECT/FROM (case insensitive) correctly" in {
    val parser = Parser
    val r = parser.parse("select * FROM table t")
    r should not be None
  }
  "Parser" should "parse single expression correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT column1 FROM table")
    r should not be None
  }
  "Parser" should "parse explicit columns correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT column1, ident.column2 FROM table")
    r should not be None
  }

  "Parser" should "parse aggregate functions correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT SUM(column1), MIN(ident.column2) FROM table")
    r should not be None
  }

  "Parser" should "parse aliased expression correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT SUM(column1) AS theSum, MIN(ident.column2) AS minimum FROM table")
    r should not be None
  }

  "Parser" should "parse arithmetic operations correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT SUM(column1) / 10, MIN(ident.column2) + 125.50 FROM table")
    r should not be None
  }

  "Parser" should "parse GROUP BY with HAVING correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT column1 FROM table GROUP BY column1, table.column2 HAVING SUM(table.column3) > 12345")
    r should not be None
  }

  "Parser" should "parse WHERE correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT column1 FROM table WHERE column2 > 2.51 AND 1 != 0 AND table.column3 BETWEEN 5 AND 10")
    r should not be None
  }

  "Parser" should "parse dates correctly" in {
    val parser = Parser
    val r = parser.parse("SELECT * FROM lineitem WHERE DATE '1998-09-02'")
    r should not be None
  }
}
