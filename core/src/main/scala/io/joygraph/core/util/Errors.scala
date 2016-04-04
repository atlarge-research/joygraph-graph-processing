package io.joygraph.core.util

import scala.compat.Platform._

object Errors {
  def messageAndStackTraceString(t : Throwable) : String = {
    s"$t: {t.getMessage} \n${t.getStackTrace.mkString("", EOL, EOL)}"
  }
}
