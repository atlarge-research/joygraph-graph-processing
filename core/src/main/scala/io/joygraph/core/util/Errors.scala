package io.joygraph.core.util

import scala.compat.Platform._

object Errors {
  def messageAndStackTraceString(t : Throwable) : String = {
    s"$t: \n${t.getStackTrace.mkString("", EOL, EOL)}"
  }
}
