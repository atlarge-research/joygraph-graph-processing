package io.joygraph.analysis.figure

import scala.collection.mutable.ArrayBuffer

object TournamentScoresTableFigure {

  def newBuilder : Builder = {
    new Builder()
  }

  class Builder {

    type Row = (String, Double)
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

    def result(policy : String, score : Double) : Builder = {
      _rows += ((policy, score))
      this
    }

    def build(): String = {
      val columns = Array(
        "score"
      )

      val rows = _rows.map {
        case (policy, score) =>
          "%s & %.1f\\\\\n".format(policy, score)
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
         |\\caption{${_partialCaption} for ${_rows.map(_._1).fold("")(_ + ", " + _)} on dataset ${_dataset} and algorithm ${_algorithm}}
         |\\label{${_labelPrefix}-tournament-${_algorithm}-${_dataset}}
         |\\end{table}
        """.stripMargin
    }

  }
}
