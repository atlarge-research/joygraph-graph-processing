package io.joygraph.analysis.figure

import io.joygraph.analysis.figure.DiagramFigure.Builder
import io.joygraph.analysis.figure.PythonTools.createPythonArray

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

object MultiDiagramFigure {

  def builder: FigureBuilder = {
    new FigureBuilder
  }

  def diagramBuilder: DiagramBuilder = {
    new DiagramBuilder
  }

  class DiagramBuilder {
    type xValue = Double
    type yValue = Double
    type yError = (Double, Double) // absDiff from y value (min, max)
    type Properties = (xValue, yValue, yError)
    private var _vals : ArrayBuffer[Properties] = ArrayBuffer.empty[Properties]
    private var _yAxisLabel : String = _
    private var _xAxisLabel : String = _
    private var _diagramTitle : String = _

    def values(vals : Properties) : DiagramBuilder = {
      _vals += vals
      this
    }

    def yAxisLabel(label : String) : DiagramBuilder = {
      _yAxisLabel = label
      this
    }

    def xAxisLabel(label : String) : DiagramBuilder = {
      _xAxisLabel = label
      this
    }

    def diagramTitle(title : String) : DiagramBuilder = {
      _diagramTitle = title
      this
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

    def build(): String = {
      s"""
        |plt.xlabel('${_xAxisLabel}')
        |plt.ylabel('${_yAxisLabel}')
        |plt.title("${_diagramTitle}")
        |plt.xticks(arange(${_vals.max}), ${createXValues()})
        |
        |plt.plot(${createXValues()}, ${createYValues()}, 'ro')
      """.stripMargin
    }
  }

  class FigureBuilder {

    private var _name : String = _
    private var _diagramBuilders : ArrayBuffer[DiagramBuilder] = ArrayBuffer.empty[DiagramBuilder]

    def fileName(name : String) : FigureBuilder = {
      _name = name
      this
    }

    def sanitizeInput() = {
      // noop
    }

    def addSubPlot(diagramBuilder: DiagramBuilder) = {
      _diagramBuilders += diagramBuilder
    }

    private def createSubPlots(): String = {
      val cols = 1
      val rows = _diagramBuilders.size
      var fignum = 0
      _diagramBuilders.map {
        diagramBuilder =>
          fignum += 1
          s"""
            |plt.subplot($rows$cols$fignum)
            |${diagramBuilder.build()}
            |
          """.stripMargin
      }.reduce(_ + _)
    }

    def build() : String = {
      // create python script
      sanitizeInput()
      val pyScript = s"""
        |import numpy as np
        |import matplotlib.pyplot as plt
        |
        |${createSubPlots()}
        |
        |plt.savefig("${_name}.pdf")
        |""".stripMargin

      /// execute pyscript
      val scriptLocation = File.makeTemp()
      scriptLocation.writeAll(pyScript)
      scriptLocation.setExecutable(executable = true)
      new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
      _name
    }

  }
}
