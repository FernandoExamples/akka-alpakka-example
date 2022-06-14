package utils

import org.slf4j.{Logger, LoggerFactory}

trait LogSupport {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
