package io.joygraph.analysis.figure

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
    type xValue = Int
    type yValue = String
    type yError = (Double, Double) // absDiff from y value (min, max)
    type Properties = (xValue, yValue, yError)
    private var _vals : ArrayBuffer[Properties] = ArrayBuffer.empty[Properties]
    private var _yAxisLabel : String = ""
    private var _xAxisLabel : String = ""

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
        createPythonArray(_vals.map{_._2})
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

    def build(): String =
      s"""
        |plt.xlabel('${_xAxisLabel}')
        |plt.ylabel('${_yAxisLabel}')
        |plt.xticks(${createXValues()}, ${createXValues()}, rotation=30)
        |plt.plot(${createXValues()}, ${createYValues()}, 'ro')
      """.stripMargin
  }

  class FigureBuilder {

    private var _name : String = _
    private var _diagramBuilders : ArrayBuffer[DiagramBuilder] = ArrayBuffer.empty[DiagramBuilder]
    private var _diagramTitle : String = ""

    def diagramTitle(title : String) : FigureBuilder = {
      _diagramTitle = title
      this
    }

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
            |plt.subplot($rows,$cols,$fignum)
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
        |plt.figure(figsize=(10, 20))
        |plt.title("${_diagramTitle}")
        |${createSubPlots()}
        |plt.tight_layout()
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
