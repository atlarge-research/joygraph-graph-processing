package io.joygraph.analysis.figure

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

object DiagramFigure {

  def newBuilder : Builder = new Builder
  def labelOnlyNewBuilder : LabelOnlyBuilder = new LabelOnlyBuilder

  class LabelOnlyBuilder {
    type Label = String
    type yValue = Double
    type yError = (Double, Double) // absDiff from y value (min, max)
    type PropertiesWithXMapped = (Label, Int, yValue, yError)
    type Properties = (Label, yValue, yError)
    private var _vals : ArrayBuffer[Properties] = ArrayBuffer.empty[Properties]
    private var _name : String = _
    private var _newVals : ArrayBuffer[PropertiesWithXMapped] = _
    private var _yAxisLabel : String = _
    private var _xAxisLabel : String = _
    private var _diagramTitle : String = ""
    private var _sortByY : Boolean = true
    private var _manualAxis : Option[(String, String, String, String)] = None
    private var _manualXAxis : Option[(String, String)] = None
    private var _logScale : Boolean = true

    def values(vals : Properties) : LabelOnlyBuilder = {
      _vals += vals
      this
    }

    def fileName(name : String) : LabelOnlyBuilder = {
      _name = name
      this
    }

    def yAxisLabel(label : String) : LabelOnlyBuilder = {
      _yAxisLabel = label
      this
    }

    def xAxisLabel(label : String) : LabelOnlyBuilder = {
      _xAxisLabel = label
      this
    }

    def diagramTitle(title : String) : LabelOnlyBuilder = {
      _diagramTitle = title
      this
    }

    def sortByY(sortByY : Boolean) : LabelOnlyBuilder = {
      _sortByY = sortByY
      this
    }

    def manualAxis(xMin : String, xMax : String, yMin : String, yMax : String) : LabelOnlyBuilder = {
      _manualAxis = Some((xMin, xMax, yMin, yMax))
      this
    }

    def manualXAxis(xMin : String, xMax : String) : LabelOnlyBuilder = {
      _manualXAxis = Some((xMin, xMax))
      this
    }

    def logScale(logScale : Boolean): LabelOnlyBuilder = {
      _logScale = logScale
      this
    }

    private def createManualAxis() : String = {
      var output = ""
      _manualAxis.foreach {
        case (xMin, xMax, yMin, yMax) => {
          output = "plt.axis([%s, %s, %s, %s])".format(xMin, xMax, yMin, yMax)
        }
      }
      output
    }

    private def createManualXAxis() : String = {
      var output = ""
      _manualXAxis.foreach {
        case (xMin, xMax) => {
          output = "plt.xlim((%s, %s))".format(xMin, xMax)
        }
      }
      output
    }

    private def createLogScale() : String = {
      if (_logScale) {
        "plt.yscale('log')"
      } else {
        ""
      }
    }

    def sanitizeInput() = {
      // we have labels
      // we assume we don't have x values in this case
      // we sort the vals by Y
      if (_sortByY) {
        _vals = _vals.sortBy(_._2)
      }
      // we mapped the x values
      _newVals = _vals.zipWithIndex.map {
        case (value , index) => (value._1, index, value._2, value._3)
      }
    }

    def createPythonArray(iterable : Iterable[String]) : String = {
      "[" + iterable.reduce(_ + "," + _) + "]"
    }

    def createXValues(): String = {
      if (_newVals.isEmpty) {
        "[]"
      } else {
        createPythonArray(_newVals.map(_._2.toString))
      }
    }

    def createYValues() : String = {
      if (_newVals.isEmpty) {
        "[]"
      } else {
        createPythonArray(_newVals.map(_._3.toString))
      }
    }

    def createErrorValues() : String = {
      if (_newVals.isEmpty) {
        "[]"
      } else {
        val mins = _newVals.map(_._4._1.toString)
        val maxs = _newVals.map(_._4._2.toString)
        "[" + createPythonArray(mins) + "," + createPythonArray(maxs) + "]"
      }
    }

    def createLabels() : String = {
      if (_newVals.isEmpty) {
        "[]"
      } else {
        createPythonArray(_newVals.map("\"" + _._1 + "\""))
      }
    }

    def build() : String = {
      // create python script lolol
      sanitizeInput()
      val pyScript = s"""
        |import numpy as np
        |import matplotlib.pyplot as plt
        |
        |plt.rc('font', size=15)
        |
        |x = ${createXValues()}
        |y = ${createYValues()}
        |err = ${createErrorValues()}
        |labels = ${createLabels()}
        |
        |plt.xlabel('${_xAxisLabel}')
        |plt.ylabel('${_yAxisLabel}')
        |plt.title("${_diagramTitle}")
        |${createManualAxis()}
        |${createManualXAxis()}
        |${createLogScale()}
        |
        |if len(labels) > 0:
        |\tplt.xticks(x, labels, rotation=30)
        |\tplt.margins(0.2)
        |
        |plt.subplots_adjust(bottom=0.2)
        |plt.errorbar(x, y, yerr=err)
        |plt.savefig("${_name}.pdf")
      """.stripMargin

      /// execute pyscript
      val scriptLocation = File(s"${_name}.py")
      scriptLocation.writeAll(pyScript)
      scriptLocation.setExecutable(executable = true)
      new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
      _name
    }

  }

  class Builder {
    type xValue = Double
    type yValue = Double
    type yError = (Double, Double) // absDiff from y value (min, max)
    type Properties = (xValue, yValue, yError)
    private var _vals : ArrayBuffer[Properties] = ArrayBuffer.empty[Properties]
    private var _name : String = _
    private var _yAxisLabel : String = _
    private var _xAxisLabel : String = _
    private var _diagramTitle : String = _


    def values(vals : Properties) : Builder = {
      _vals += vals
      this
    }

    def fileName(name : String) : Builder = {
      _name = name
      this
    }

    def yAxisLabel(label : String) : Builder = {
      _yAxisLabel = label
      this
    }

    def xAxisLabel(label : String) : Builder = {
      _xAxisLabel = label
      this
    }

    def diagramTitle(title : String) : Builder = {
      _diagramTitle = title
      this
    }

    def sanitizeInput() = {
      // noop
    }

    def createPythonArray(iterable : Iterable[String]) : String = {
      "[" + iterable.reduce(_ + "," + _) + "]"
    }

    def createXValues(): String = {
      if (_vals.isEmpty) {
        "[]"
      } else {
        createPythonArray(_vals.map(_._1.toString))
      }
    }

    def createYValues() : String = {
      if (_vals.isEmpty) {
        "[]"
      } else {
        createPythonArray(_vals.map(_._2.toString))
      }
    }

    def createErrorValues() : String = {
      if (_vals.isEmpty) {
        "[]"
      } else {
        val mins = _vals.map(_._3._1.toString)
        val maxs = _vals.map(_._3._2.toString)
        "[" + createPythonArray(mins) + "," + createPythonArray(maxs) + "]"
      }
    }

    def build() : String = {
      // create python script
      sanitizeInput()
      val pyScript = s"""
        |import numpy as np
        |import matplotlib.pyplot as plt
        |
        |x = ${createXValues()}
        |y = ${createYValues()}
        |err = ${createErrorValues()}
        |
        |plt.xlabel('${_xAxisLabel}')
        |plt.ylabel('${_yAxisLabel}')
        |plt.title("${_diagramTitle}")
        |
        |plt.errorbar(x, y, xerr=err)
        |plt.savefig("${_name}.pdf")
        |""".stripMargin

      /// execute pyscript
      val scriptLocation = File(s"${_name}.py")
      scriptLocation.writeAll(pyScript)
      scriptLocation.setExecutable(executable = true)
      new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
      _name
    }

  }
}
