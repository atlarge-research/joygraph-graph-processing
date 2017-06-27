package io.joygraph.analysis.matplotlib

import scala.reflect.io.File

case class VariabilityBarPerStep
(
   numSuperSteps : Long,
   means : Iterable[Double],
   errors : Iterable[Double]
) {

  private[this] def generatePyArray(arr : Iterable[Any]) : String = {
    "[" + arr.map(_.toString).reduce(_ + "," + _) + "]"
  }

  def createChart
  (
    outputPath : String,
    xLabel : String,
    yLabel : String,
    yLegend : String
  ) : Unit = {
    val meansPyArray = generatePyArray(means)
    val errorsPyArray = generatePyArray(errors)
    val script =
      s"""
         |import numpy as np
         |import matplotlib.pyplot as plt
         |
         |numSupersteps = $numSuperSteps
         |yAverageProcSpeed = $meansPyArray
         |yProcSpeedError = $errorsPyArray
         |
         |yProcSpeedError = list(map(lambda x: x / 1000, yProcSpeedError))
         |yAverageProcSpeed = list(map(lambda x: x / 1000, yAverageProcSpeed))
         |
         |barWidth = 0.35
         |steps = np.arange(0, numSupersteps, 1)
         |
         |fig = plt.figure()
         |barChart = fig.add_axes((0.1, 0.1, 0.8, 0.8))
         |barChart.set_xlim([-0.1, max(steps) + 0.5])
         |p1 = barChart.bar(steps, yAverageProcSpeed, barWidth, color='r', yerr = yProcSpeedError)
         |barChart.set_xticks(steps + barWidth/2.)
         |barChart.set_xticklabels(steps)
         |barChart.set_xlabel('$xLabel')
         |barChart.set_ylabel('$yLabel')
         |barChart.legend((p1[0]), ('$yLegend'), loc = 0)
         |plt.savefig("$outputPath")
       """.stripMargin

    val scriptLocation = File.makeTemp()
    scriptLocation.writeAll(script)
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