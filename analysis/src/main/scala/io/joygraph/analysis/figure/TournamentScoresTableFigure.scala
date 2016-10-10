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
         | ${rows.reduce(_ + _)}
         | \\hline
         |\\end{tabular}
         |\\caption{Tournament scores for ${_rows.map(_._1).reduce(_ + ", " + _)} on dataset ${_dataset} and algorithm ${_algorithm}}
         |\\end{table}
        """.stripMargin
    }

  }
}
