package io.joygraph.analysis.figure

import io.joygraph.analysis.autoscale.metrics.{AccuracyMetric, InstabilityMetric, WrongProvisioningMetric}

import scala.collection.mutable.ArrayBuffer

object ElasticTableFigure {
  def newBuilder : Builder = {
    new Builder()
  }

  class Builder {

    type Row = (String, AccuracyMetric, WrongProvisioningMetric, InstabilityMetric)
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

    def policyResult(policy : String, accuracyMetric: AccuracyMetric, wrongProvisioningMetric: WrongProvisioningMetric, instabilityMetric: InstabilityMetric) : Builder = {
      _rows += ((policy, accuracyMetric, wrongProvisioningMetric, instabilityMetric))
      this
    }

    def build(): String = {
      val columns = Array(
        "$a_U$", "$a_O$", "$\\bar{a}_U$", "$\\bar{a}_O$", // accuracy metric
        "$t_U$", "$t_O$", // wrongProvisioning
        "$i$", "$i'$" // instability
      )

//      val rows =
//        _rows.map {
//          case (policy, a, b, c) =>
//            (policy, a.scaleBy(100.0), b.scaleBy(100.0), c.scaleBy(100.0))
//        }.map {
////        case (policy, AccuracyMetric(aU, aO, aUn, aOn, aOm), WrongProvisioningMetric(wU, wO), InstabilityMetric(i, i2)) =>
////          "%s & %.4f & %.4f & %.4f & %.4f & %.4f & %.4f & %.4f & %.4f \\\\\n".format(policy, aU, aO, aUn, aOn, wU, wO, i, i2)
//        case (policy, AccuracyMetric(aU, aO, aUn, aOn, aOm), WrongProvisioningMetric(wU, wO), InstabilityMetric(i, i2)) =>
//          "%s & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f \\\\\n".format(policy, aU, aO, aUn, aOn, wU, wO, i, i2)
//      }

      // affix grouping
      val BASELINE = 0
      val OWN = 2
      val GENERAL = 1

      val groupedRows = _rows.groupBy {
        case (policy, _,_,_) =>
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
          case (policy, a, b, c) =>
            (policy, a.scaleBy(100.0), b.scaleBy(100.0), c.scaleBy(100.0))
        })
        .mapValues(x => x.map {
          case (policy, AccuracyMetric(aU, aO, aUn, aOn, aOm), WrongProvisioningMetric(wU, wO), InstabilityMetric(i, i2)) =>
            "%s & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f & %.2f \\\\\n".format(policy, aU, aO, aUn, aOn, wU, wO, i, i2)
        }).mapValues(_.reduce(_ + _))

      s"""
         |\\begin{table}[H]
         |\\begin{tabular}{l ${columns.indices.map(_ => "c").reduce(_ + " " + _)}}
         | & ${columns.reduce(_ + " & " + _)} \\\\
         | \\hline
         | \\hline
         | ${_algorithm} ${_dataset} \\\\
         | \\hline
         | \\hline
         |  ${groupedRows.values.reduce(_ + "\\hline\\hline\n" + _)}
         | \\hline
         |\\end{tabular}
         |\\caption{Autoscaler metrics for ${_rows.map(_._1).reduce(_ + ", " + _)} on dataset ${_dataset} and algorithm ${_algorithm}}
         |\\label{autoscaler-${_algorithm}-${_dataset}}
         |\\end{table}
        """.stripMargin
    }

  }
}
