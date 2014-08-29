package ch.epfl.data
package legobase

class NextContainer[T](val current: T, var next: NextContainer[T])
class NextKeyContainer[K, T](val current: T, var key: K, var next: NextKeyContainer[K, T])
