package ch.epfl.data
package legobase
package utils
package parser

import scala.util.parsing.combinator.RegexParsers

object Parser extends RegexParsers {
    def parseSelectStatement: Parser[SelectStatement] = (
        "SELECT" ~ parseSelectList ~ "FROM" ~ parseTableExpression ~ ";".? ^^ { case _ ~ sl ~ _ ~ te => SelectStatement(sl, te) }
    )

    def parseSelectList: Parser[SelectList] = (
        "*" ^^ { case _ => AllColumns() }
        | rep1sep(parseValueExpression, ",") ^^ { case lst => ColumnList(lst) }
    )

    // TODO: parseValueExpression

    def parseTableExpression: Parser[TableExpression] = (
        parseFromClause
        ~ ("WHERE" ~> parseWhereClause).?
        ~ ("GROUP BY" ~> parseGroupClause).?
        ~ ("ORDER BY" ~> parseOrderClause).? ^^
        { case tr ~ wh ~ grp ~ ord => TableExpression(tr, wh, grp, ord) }
    )

    def parseFromClause: Parser[FromClause] = (
        rep1sep(parseTableReference ~ , ",") ^^ { case lst => FromClause(lst) }
    )

    def parseTableReference: Parser[TableReference] = (
        name ^^ { case name => PrimaryTable(name) }
    )

    // TODO: parseWhereClause

    // TODO: parseGroupClause

    def parseOrderClause: Parser[OrderClause] = (
        rep1sep(parseOrderKey ~ , ",") ^^ { case keys => OrderClause(keys) }
    )

    def parseOrderKey: Parser[OrderKey] = (
        parseValueExpression <~ "ASC".? ^^ { case v => AscendingOrderKey(v) }
        | parseValueExpression <~ "DESC" ^^ { case v => DescendingOrderKey(v) }
    )
