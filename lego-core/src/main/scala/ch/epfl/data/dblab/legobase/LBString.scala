package ch.epfl.data
package dblab

/**
 * The package object for legobase package which rewires LBString to
 * [[sc.pardis.shallow.OptimalString]]
 */
package object legobase {
  type LBString = sc.pardis.shallow.OptimalString
  val LBString = sc.pardis.shallow.OptimalString
}
