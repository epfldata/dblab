package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
import ch.epfl.data.sc.pardis.types.Tpe
/**
 * @author Parand Alizadeh
 */
object CalcCompiler {

  def compileMap(todot: todot, computedelta: Boolean) : (List[todot], Ds) = {
    ???
  }

  def compileTlqs(calcQueries: List[CalcQuery]): (List[todot], List[CalcQuery]) = {
    val (todolist, toplevelqueries) = calcQueries.map(x => {

        val qschema = schemaOfExpression(x.expr)
        val qtype = typeOfExpression(x.expr)
        val dsname = External(x.name, qschema._1, qschema._2, qtype, None)
        (todot(1, Ds(dsname, x.expr), false), CalcQuery(x.name, dsname))}).unzip
    (todolist, toplevelqueries)

  }

  def compile(calcQueris: List[CalcQuery]): (Plan, List[CalcQuery]) = {
    var (todolist, tlqlist) = compileTlqs(calcQueris)

    while(todolist.length > 0){
      val nextds = todolist.head
      todolist = todolist.tail
      val depth = nextds.depth
      val todoo = nextds.ds
      val skip = nextds.b

      val computedelta = true

      val (newtodos, compiledds) = compileMap(nextds, computedelta)
    }
  }
}
