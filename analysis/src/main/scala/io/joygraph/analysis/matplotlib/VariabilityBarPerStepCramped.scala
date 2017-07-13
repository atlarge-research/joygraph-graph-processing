package io.joygraph.analysis.matplotlib

import io.joygraph.analysis.figure.PythonTools

import scala.reflect.io.File

case class VariabilityBarPerStepCramped
(
   xTickLabels : Iterable[String],
   means : Iterable[Double],
   errors : Iterable[Double]
) {

  def createChart
  (
    outputPath : String,
    xLabel : String,
    yLabel : String
  ) : Unit = {

    // TODO do we really want to cast NaN to float ('nan') ?
    val meansPyArray = PythonTools.createPythonArray(means.map{ x =>
      if (java.lang.Double.isNaN(x)) "float('nan')" else x.toString
    })
    val errorsPyArray = PythonTools.createPythonArray(errors.map{ x =>
      if (java.lang.Double.isNaN(x)) "float('nan')" else x.toString
    })
    val xTickLabelsPyArray = PythonTools.createPythonArray(xTickLabels)
    val script =
      s"""
         |import numpy as np
         |import matplotlib.pyplot as plt
         |
         |yAverageProcSpeed = $meansPyArray
         |yProcSpeedError = $errorsPyArray
         |
         |barWidth = 0.1
         |steps = np.arange(0, ${xTickLabels.size}, 1)
         |
         |fig = plt.figure()
         |barChart = fig.add_axes((0.15, 0.1, 0.8, 0.8))
         |barChart.set_xlim([-0.15, max(steps) + 0.5])
         |p1 = barChart.bar(steps, yAverageProcSpeed, barWidth, color='r', yerr = yProcSpeedError)
         |barChart.set_xticks(steps)
         |barChart.set_xticklabels($xTickLabelsPyArray)
         |barChart.set_xlabel('$xLabel')
         |barChart.set_ylabel('$yLabel')
         |plt.savefig("$outputPath.pdf")
       """.stripMargin

    val scriptLocation = File.makeTemp()
    scriptLocation.writeAll(script)
    println(s"attempting to $scriptLocation")
    scriptLocation.setExecutable(executable = true)
    new ProcessBuilder().command("/usr/bin/python", scriptLocation.toString).start().waitFor()
  }

  def createLatex(relativeLatexPathPrefix: String, fileName : String, caption : String, label : String): Unit = {
    s"""
       |\\begin{figure}[H]
       | \\centering
       | \\includegraphics[width=1.0\\linewidth]{$relativeLatexPathPrefix/$fileName}
       |\\caption{$caption}
       |\\label{$label}
       |\\end{figure}
     """.stripMargin
  }
}
