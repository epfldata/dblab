package ch.epfl.data
package legobase
package utils
package ast

class SelectStatement(sl: SelectList, te: TableExpression)

class SelectList
case class AllColumns() extends SelectList
case class ColumnList(cols: List[ValueExpression]) extends SelectList

class ValueExpression
case class BooleanValueExpression() extends ValueExpression
case class RowValueExpression() extends ValueExpression
case class CommonValueExpression() extends ValueExpression

class TableExpression() //TODO: WIP
//class TableExpression(from: FromClause, where: WhereClause, group: GroupClause, having: HavingClause, order: OrderClause)

class FromClause
case class TableReferences(tables: List[TableReference]) extends FromClause
case class JoinedTables() extends FromClause

class TableReference
case class PrimaryTable(name: String) extends TableReference
case class Subquery(sel: SelectStatement) extends TableReference
