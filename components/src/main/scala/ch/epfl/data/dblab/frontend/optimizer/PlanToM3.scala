package ch.epfl.data.dblab.frontend.optimizer

import ch.epfl.data.dblab.frontend.parser.CalcAST._
import ch.epfl.data.dblab.frontend.optimizer.CalcUtils._
import ch.epfl.data.dblab.schema.Schema

/**
 * Created by parand on 11/28/17.
 */
object PlanToM3 {

  def getStatement(progT: ProgT, eventT: EventT, name: String): Option[StmtT] = {
    val trigger = getTrigger(progT, eventT)
    val res = trigger.stmt.find(stmt => externalsOfExpression(stmt.targetMap).head == name)
    res
  }

  def getTrigger(progT: ProgT, eventT: EventT): M3Trigger = {
    val r = progT.triggers.find(x => eventEquals(eventT, x.event))
    val res = r match {
      case Some(x) => x
      case None    => null
    }
    res
  }

  def addStmt(progT: ProgT, eventT: EventT, stmtT: StmtT): ProgT = {
    val relv = eventVars(eventT)
    try {
      val trigger = getTrigger(progT, eventT)
      val trigRelv = eventVars(trigger.event)
      val safeMapping = findSafeVarMapping(findSafeVarMapping(relv.zip(trigRelv), stmtT.updateExpr), stmtT.targetMap)
      val trigStatements = trigger.stmt :+ StmtT(renameVars(safeMapping, stmtT.targetMap), stmtT.updateType, renameVars(safeMapping, stmtT.updateExpr))
      val trigs = progT.triggers.foldLeft(List.empty[M3Trigger])((acc, cur) => {
        if (getTrigger(progT, eventT).equals(cur))
          acc :+ M3Trigger(cur.event, trigStatements)
        else
          acc :+ cur
      })
      val p = ProgT(progT.queries, progT.maps, trigs, progT.db)
      p

    } catch {
      case e: Exception => throw new Exception("Adding statement for an event that has not been established")

    }
  }
  def addView(progT: ProgT, view: Ds): ProgT = {
    val maps = progT.maps :+ DSView(view)
    val p = ProgT(progT.queries, maps, progT.triggers, progT.db)
    p
  }

  def defaultTriggers(): List[M3Trigger] = {
    val res = List(SystemInitializedEvent()).map(x => M3Trigger(x, List.empty[StmtT]))
    res
  }

  def init(db: Schema): ProgT = {
    //TODO condition of partition
    val (dbStreams, dbTables) = db.tables.partition(tableHasStream)
    val queries = List.empty[CalcQuery]
    val maps = dbTables.map(x => DSTable(x)).toList
    val (a, b, c) = dbStreams.map(x => (InsertEvent(x), DeleteEvent(x), BatchUpdate(x.name))).unzip3
    val alllist = (a ++ b) ++ c
    val triggers = alllist.map(x => M3Trigger(x, null)) ++ defaultTriggers()
    val p = ProgT(queries, maps, triggers.toList, db)
    p
  }

  def finalize(progT: ProgT): ProgT = {
    val nonEmtyTrig = progT.triggers.filter(x => {
      x.event match {
        case InsertEvent(_) | DeleteEvent(_) | BatchUpdate(_) => (x.stmt.length != 0)
        case _ => true
      }
    })

    val usedRels = multiunion(progT.maps.map(x => {
      x match {
        case DSView(view) => relsOfExpr(view.dsdef)
        case _            => List()
      }
    }))

    val nonEmptRels = progT.db.tables.filter(x => usedRels.contains(x.name))
    val nonEmtpyDb = {
      if (nonEmptRels.length != 0)
        List(nonEmptRels)
      else
        List()
    }
    val db = Schema(nonEmptRels, progT.db.stats, progT.db.functions)
    val p = ProgT(progT.queries, progT.maps, nonEmtyTrig, db)
    p
  }

  def sortProg(progT: ProgT): ProgT = {
    var res = List.empty[M3Trigger]
    for (trigger <- progT.triggers) {
      val (localStmts, triggerStms) = trigger.stmt.partition(x => deltaRelsOfExpression(x.updateExpr).length != 0)
      val localExternals = localStmts.map(x => externalsOfExpression(x.targetMap)).zip(localStmts)
      val replaceStmts = triggerStms.filter(x => x.updateType == ReplaceStmt)
      val replaceTargetNames = replaceStmts.map(x => externalsOfExpression(x.targetMap).head)
      //TODO
      val graph = triggerStms.map(stmt => {
        val e = expandDsName(stmt.targetMap)
        val ivcNames = e.meta match { case None => List() case Some(unwrappedIVC) => externalsOfExpression(unwrappedIVC) }
        val updateNames = (ivcNames.union(externalsOfExpression(stmt.updateExpr))).diff(localExternals)
        if (stmt.updateType == UpdateStmt)
          (e.name, updateNames.union(replaceTargetNames))
        else
          (e.name, updateNames.intersect(replaceTargetNames))
      })

      val newStmtOrder = topoSort(graph).foldLeft(List.empty[StmtT])((acc, cur) => {
        val nextStmt = getStatement(progT, trigger.event, cur)
        nextStmt match {

          case Some(s) => (acc :+ s)
          case None    => acc
        }
      })

      val t = M3Trigger(trigger.event, localStmts ::: newStmtOrder)
      res = res :+ t
    }
    val p = ProgT(progT.queries, progT.maps, res, progT.db)
    p
  }
  def planToM3(db: Schema, plan: Plan): ProgT = {
    var prog = init(db)
    for (x <- plan.list) {
      prog = addView(prog, x.description)
      for (y <- x.triggers) {
        prog = addStmt(prog, y.event, y.stmt)
      }
    }
    finalize(sortProg(prog))

  }
}
