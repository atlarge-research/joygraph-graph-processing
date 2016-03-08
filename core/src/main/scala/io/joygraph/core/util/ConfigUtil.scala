package io.joygraph.core.util

import com.typesafe.config.Config

object ConfigUtil {
  def clazz(name : String)(implicit conf : Config) : Class[_] = {
    Class.forName(name)
  }
}
