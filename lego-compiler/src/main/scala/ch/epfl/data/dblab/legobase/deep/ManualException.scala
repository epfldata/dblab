package ch.epfl.data
package dblab.legobase
package deep

import java.io.PrintStream
import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._

/** This deep class is manually created */
trait ExceptionOps extends Base {
  type Exception = java.lang.Exception
  implicit val typeException: TypeRep[Exception] = ExceptionIRs.ExceptionType

  def __newException(msg: Rep[String]) = new Exception(msg.toString)
}

object ExceptionIRs extends Base {
  case object ExceptionType extends TypeRep[Exception] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = ExceptionType
    val name = "Exception"
    val typeArguments = Nil
  }
  implicit val typeException: TypeRep[Exception] = ExceptionType
  type Exception = java.lang.Exception
}

