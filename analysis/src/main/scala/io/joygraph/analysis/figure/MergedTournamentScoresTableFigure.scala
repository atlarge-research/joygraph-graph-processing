package io.joygraph.analysis.figure

import scala.collection.mutable.ArrayBuffer

object MergedTournamentScoresTableFigure {

  def newBuilder : Builder = {
    new Builder()
  }

  class Builder {

    type Row = ((String, Double),(String, Double),(String, Double))
    private var _algorithm : String = _
    private var _dataset : String = _
    private val _rows = ArrayBuffer.empty[Row]
    private var _partialCaption : String = _
    private var _labelPrefix : String = _

    def labelPrefix(labelPrefix : String) : Builder = {
      _labelPrefix = labelPrefix
      this
    }

    def partialCaption(partialCaption : String) : Builder = {
      _partialCaption = partialCaption
      this
    }

    def algorithm(algorithm : String) : Builder = {
      _algorithm = algorithm
      this
    }

    def dataSet(dataset : String) : Builder = {
      _dataset = dataset
      this
    }

    def result(row : Row) : Builder = {
      _rows += row
      this
    }

    def build(): String = {
      val columns = Array(
        "$score_{e}$",
        "",
        "$score_{p}$",
        "",
        "$score_{c}$"
      )

      val rows = _rows.map {
        case ((policy, score),(policy2, score2),(policy3, score3)) =>
          "%s & %.1f & %s & %.1f & %s & %.1f\\\\\n".format(policy, score, policy2, score2, policy3, score3)
      }

      s"""
         |\\begin{table}[H]
         |\\begin{tabular}{l ${columns.indices.map(_ => "r").reduce(_ + " " + _)}}
         | & ${columns.reduce(_ + " & " + _)} \\\\
         | \\hline
         | \\hline
         | ${_algorithm} ${_dataset} \\\\
         | \\hline
         | \\hline
         | ${rows.fold("")(_ + _)}
         | \\hline
         |\\end{tabular}
         |\\caption{${_partialCaption} for ${_rows.map(_._1._1).reduce(_ + ", " + _)} on dataset ${_dataset} and algorithm ${_algorithm}}
         |\\label{${_labelPrefix}-tournament-${_algorithm}-${_dataset}}
         |\\end{table}
        """.stripMargin
    }

  }
}
