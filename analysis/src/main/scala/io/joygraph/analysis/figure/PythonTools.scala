package io.joygraph.analysis.figure

object PythonTools {
  def createPythonArray(iterable : Iterable[String]) : String = {
    if (iterable.isEmpty) {
      "[]"
    } else {
      "[" + iterable.reduce(_ + "," + _) + "]"
    }
  }
}
