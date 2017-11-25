package io.joygraph.analysis.figure

import io.joygraph.analysis.performance.PerformanceMetric

import scala.collection.mutable.ArrayBuffer

object GeneralProcessingTableFigure {
  def newBuilder : Builder = {
    new Builder()
  }

  class Builder {

    type Row = (String, PerformanceMetric, Double)
    private var _algorithm : String = _
    private var _dataset : String = _
    private val _rows = ArrayBuffer.empty[Row]

    def algorithm(algorithm : String) : Builder = {
      _algorithm = algorithm
      this
    }

    def dataSet(dataset : String) : Builder = {
      _dataset = dataset
      this
    }

    def result(policy : String, performanceMetric : PerformanceMetric, coefficientOfVariationProc : Double) : Builder = {
      _rows += ((policy, performanceMetric, coefficientOfVariationProc))
      this
    }

    def build(): String = {
      val columns = Array(
        "$t_{p}$ (s)", "$t_{m}$ (s)", "$t_{e}$ (s)", "$\\sum{t_{c}}$ (s)", // times
        "VPS", "EVPS", // vps and eps
        "$\\sum{t_{e}}$ (s)", // elasticity overhead
        "$\\sum{t_s}$ (s)", // pure processing time
        "$CV_{proc}$" // CV proc
      )

//      val rows = _rows.map {
//        case (policy, PerformanceMetric(pTime, mTime, machineTime, vPs, ePs, eOverhead, superStepSumTime)) =>
//          "%s & %d & %d & %d & %d & %d & %d & %d \\\\\n".format(policy, pTime, mTime, machineTime, vPs, vPs + ePs, eOverhead, superStepSumTime)
//      }

      // affix grouping
      val BASELINE = 0
      val OWN = 2
      val GENERAL = 1

      val groupedRows = _rows.groupBy {
        case (policy, metric, _) =>
          policy match {
            case "Baseline" =>
              BASELINE
            case "NP" | "WCP" | "CPU" | "CPUv2" =>
              OWN
            case _ =>
              GENERAL
          }
      }.toIndexedSeq
        .sortBy(_._1)
        .toMap
        .mapValues(x => x.map {
        case (policy, PerformanceMetric(pTime, mTime, eTime, machineTime, vPs, ePs, eOverhead, superStepSumTime), cv) =>
          "%s & %d & %d & %d & %d & %d & %d & %d & %d & %.2f\\\\\n".format(policy, pTime, mTime, eTime, machineTime, vPs, vPs + ePs, eOverhead, superStepSumTime, cv)
      }).mapValues(_.reduce(_ + _))


      s"""
         |\\begin{table}[H]
         |\\begin{tabular}{l ${columns.indices.map(_ => "r").reduce(_ + " " + _)}}
         | & ${columns.reduce(_ + " & " + _)} \\\\
         | \\hline
         | \\hline
         | ${_algorithm} ${_dataset} \\\\
         | \\hline
         | \\hline
         | ${groupedRows.values.reduce(_ + "\\hline\\hline\n" + _)}
         | \\hline
         |\\end{tabular}
         |\\caption{Performance metrics for ${_rows.map(_._1).reduce(_ + ", " + _)} on dataset ${_dataset} and algorithm ${_algorithm}}
         |\\label{perf-${_algorithm}-${_dataset}}
         |\\end{table}
        """.stripMargin
    }

  }
}
