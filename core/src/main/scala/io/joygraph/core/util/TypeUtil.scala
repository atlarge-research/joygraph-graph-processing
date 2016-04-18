package io.joygraph.core.util

object TypeUtil {
  def unitOrVoid(cls : Class[_]): Boolean = {
    cls == classOf[Unit] || cls == classOf[Void]
  }
}
