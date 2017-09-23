package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
import ch.epfl.data.dblab.schema._
import ch.epfl.data.sc.pardis.types._
/**
 * @author Parand Alizadeh
 */
object CalcCompiler {


  //TODO not sure
  def rel(db: Schema, reln: String) : Option[Table] = {
    db.tables.find(x => x.name.equals(reln))
  }
  def compileMap(todot: todot, computedelta: Boolean, dbschema: Schema) : (List[todot], Ds) = {
    val ext = todot.ds match {
      case External(name,inps,outs,tp,Some(s)) =>  External(name,inps,outs,tp,Some(s))
      case External(name,inps,outs,tp,None) =>  External(name,inps,outs,tp,None)
      case _ => throw new Exception
    }
    val todotype = ext.tp match {
      case IntType => ext.tp
      case FloatType => ext.tp
      case DoubleType => ext.tp
      case BooleanType => IntType
      case _ => throw new Exception

    }
    //TODO optimiziation removed

    val rels = relsOfExpr(ext).map(x => rel(dbschema, x))
    val (tablerels, streamrels) = rels.partition(x => x match {
      case Some(_) => true
      case None => false
    })
    //TODO where are streams?



  }

  def compileTables(rel: Rel) : Ds = {
    val mapName = External("_" + rel.name, List(), rel.vars, IntType, None)
    
  }

  def compileTlqs(calcQueries: List[CalcQuery]): (List[todot], List[CalcQuery]) = {
    val (todolist, toplevelqueries) = calcQueries.map(x => {

        val qschema = schemaOfExpression(x.expr)
        val qtype = typeOfExpression(x.expr)
        val dsname = External(x.name, qschema._1, qschema._2, qtype, None)
        (todot(1, Ds(dsname, x.expr), false), CalcQuery(x.name, dsname))}).unzip
    (todolist, toplevelqueries)

  }

  def compile(calcQueris: List[CalcQuery], dbSchema: Schema): (Plan, List[CalcQuery]) = {
    var (todolist, tlqlist) = compileTlqs(calcQueris)

    while(todolist.length > 0){
      val nextds = todolist.head
      todolist = todolist.tail
      val depth = nextds.depth
      val todoo = nextds.ds
      val skip = nextds.b

      val computedelta = true

      val (newtodos, compiledds) = compileMap(nextds, computedelta, dbSchema)
    }
  }
}
