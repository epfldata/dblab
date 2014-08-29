package ch.epfl.data
package legobase

class NextContainer[T](val current: T, var next: NextContainer[T])
