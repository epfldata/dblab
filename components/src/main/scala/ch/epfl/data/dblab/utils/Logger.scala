package ch.epfl.data
package dblab
package utils

import org.apache.log4j.{ Logger => Log4jLogger }

class Logger(klass: Class[_]) {
  val logger = Log4jLogger.getLogger(klass)

  val ALLOW_LOGGING_DEBUG = true
  val ALLOW_LOGGING_INFO = true
  val ALLOW_LOGGING_WARN = true

  def debug(msg: => AnyRef): Unit = if (ALLOW_LOGGING_DEBUG) logger.debug(msg)
  def info(msg: => AnyRef): Unit = if (ALLOW_LOGGING_INFO) logger.info(msg)
  def warn(msg: => AnyRef): Unit = if (ALLOW_LOGGING_WARN) logger.warn(msg)
}

object Logger {
  def apply(klass: Class[_]) = new Logger(klass)
  def apply[T: Manifest] = new Logger(manifest[T].runtimeClass)
}
