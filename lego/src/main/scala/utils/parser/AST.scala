package ch.epfl.data
package legobase
package utils
package parser

class SelectStatement(sl: SelectList, te: TableExpression)

class SelectList
case class AllColumns() extends SelectList
case class ColumnList(cols: List[ValueExpression]) extends SelectList

trait ValueExpression
class BooleanValueExpression() with ValueExpression
    case class BooleanTerm() extends BooleanValueExpression
    case class ORExpression(ex: BooleanValueExpression, te: BooleanTerm) extends BooleanValueExpression    
class RowValueExpression() with ValueExpression
class CommonValueExpression() with ValueExpression

class TableExpression(from: FromClause, where: WhereClause, group: GroupClause, order: OrderClause)

class FromClause(tables: List[TableReference])

class TableReference
case class PrimaryTable(name: String) extends TableReference


class OrderClause(orderKeys: List[OrderKey])

class OrderKey
case class AscendingOrderKey(key: ValueExpression) extends OrderKey
case class DescendingOrderKey(key: ValueExpression) extends OrderKey