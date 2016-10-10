package io.joygraph.analysis.debug

import scala.io.Source
import scala.reflect.io.Directory

object Analyse extends App{
  val dir = ""
  val workerFiles = Directory(dir)
  workerFiles.deepFiles.filter(_.endsWith("stdout")).foreach{ file =>
    var start = false

    Source.fromFile(file.jfile).getLines().foreach{ line =>
      if (line.startsWith("VERTICESDUMPEND")) {
        start = false
      }

      if (start) {

      }

      if (line.startsWith("VERTICESDUMPSTART")) {
        start = true
      }
    }

  }

}
