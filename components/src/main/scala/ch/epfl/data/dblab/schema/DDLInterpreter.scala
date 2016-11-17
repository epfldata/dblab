package ch.epfl.data
package dblab
package schema

import frontend.parser.DDLAST._
import utils._
import sc.pardis.types._
import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer
import sys.process._
import config.Config

/**
 * An interpreter for DDL.
 *
 * @author Yannis Klonatos
 */
class DDLInterpreter(val catalog: Catalog) {
  var currSchema: Option[Schema] = None

  // TODO -- This can go away (and thus DDLTypes becoming SCTypes) 
  // if SC types become generic enough to handle precision of SQL
  // e.g. in doubles and char types
  def ddlTypeToSCType(ddlType: AttributeType) = {
    ddlType match {
      case c: DDLInteger => IntType
      case c: DDLDecimal => DoubleType
      case c: DDLVarChar => VarCharType(c.numChars)
      case c: DDLChar    => if (c.numChars == 1) CharType else VarCharType(c.numChars) // TODO -- Make SC Char take numChars as arg
      case c: DDLDate    => DateType
    }
  }

  def annonStringToConstraint(consStr: String) = consStr match {
    case "COMPRESSED" => Compressed
  }

  def getCurrSchema = {
    currSchema match {
      case None =>
        throw new Exception("Current schema not defined. Have you used USE <SCHEMA_NAME> to specify your schema?")
      case Some(schema) => schema
    }
  }

  def interpret(ddlDef: List[_]): Schema = {
    ddlDef.foreach(ddl => ddl match {
      case UseSchema(schemaName) => {
        currSchema = Some(catalog.schemata.getOrElseUpdate(schemaName, new Schema(ArrayBuffer[Table]())))
      }
      case DropTable(tableName) => {
        val schema = getCurrSchema
        schema.tables -= schema.findTable(tableName).get
      }
      case DDLTable(tableName, cols, cons) =>
        val colDef = cols.map(c => Attribute(c.name, ddlTypeToSCType(c.datatype), c.annotations.map(annonStringToConstraint(_)))).toList
        val tablePath = (Config.datapath + tableName + ".tbl").toLowerCase
        val tableCardinality = Utilities.getNumLinesInFile(tablePath)
        getCurrSchema.tables += new Table(tableName, colDef, ArrayBuffer(),
          tablePath, tableCardinality)
        interpret(cons.toList)
        if (Config.gatherStats) {
          // Update cardinality stat for this schema
          getCurrSchema.stats += "CARDINALITY_" + tableName -> tableCardinality
          // Update stats for each column -- TODO -- This should be in the generated code but OK for now
          val colNames = cols.map(_.name)
          colNames.zipWithIndex.foreach(cn => {
            cn match {
              case (name, idx) => {
                println("Getting statistics for column " + name + " of table " + tableName + "...")
                val res = (("cut -d | -f " + (idx + 1) + " " + tablePath) #| "sort" #| "uniq" #| "wc -l").!!
                getCurrSchema.stats + "DISTINCT_" + name -> res
              }
            }
          })
        }
        getCurrSchema
      case ConstraintOp(add, cons) => add match {
        case true => /* add */
          cons match {
            case pk: DDLPrimaryKey =>
              val cpk = ddlPkToPk(pk)
              val table = getCurrSchema.findTable(pk.table).get
              table.constraints += cpk
            case fk: DDLForeignKey =>
              val table = getCurrSchema.findTable(fk.table).get
              val cfk = ddlFkToFk(fk)
              table.constraints += cfk
          }
        case false => /* drop */
          cons match {
            case pk: DDLPrimaryKey =>
              val table = getCurrSchema.findTable(pk.table).get
              table.constraints --= table.primaryKey
            case fk: DDLForeignKey =>
              val table = getCurrSchema.findTable(fk.table).get
              val cfk = ddlFkToFk(fk)
              table.constraints -= cfk
          }
      }
    })
    getCurrSchema
  }

  def ddlPkToPk(ddlPk: DDLPrimaryKey): PrimaryKey = {
    val table = getCurrSchema.findTable(ddlPk.table).get
    val primaryKeyAttrs = ddlPk.primaryKeyCols.map(pkc => table.findAttribute(pkc).get)
    PrimaryKey(primaryKeyAttrs)
  }

  def ddlFkToFk(ddlFk: DDLForeignKey): ForeignKey = {
    val table = getCurrSchema.findTable(ddlFk.table).get
    // FIXME does not reference the columns of the other table
    val foreignKeyAttrs = ddlFk.foreignKeyCols.map(x => (x, x)) //.map(fkc => table.findAttribute(fkc).get)
    ForeignKey( /*ddlFk.foreignKeyName, */ ddlFk.table, ddlFk.foreignTable, foreignKeyAttrs)
  }
}