package io.joygraph.analysis

import scala.reflect.io.Directory

object FigureGenerator extends App {
  val baseResultDirectory = "/home/sietse/Documents/experimental-results/elastic"
  val bfsResultDirs = baseResultDirectory + "/BFS"
  val prResultDirs = baseResultDirectory + "/PR"
  val wccResultDirs = baseResultDirectory + "/WCC"
  val gr26ResultDirs = baseResultDirectory + "/GR26"

  val resultsDirs = Iterable(
    bfsResultDirs,
    prResultDirs,
    wccResultDirs
  ).map(Directory(_)).flatMap(_.dirs.map(_.toFile.toString()))

  val results = ParseResultDirectories(resultsDirs)
  results.experiments.foreach { x =>
    println(x.createPerformanceTableWithAverages() + x.createElasticTableWithAverages() + x.createTournamentScoreTable())
    val namingTemplate = (element : String) => s"${x.dataSet}-${x.algorithm}-$element"
    println(x.createFigureVerticesPerSecondFigure(z => z.verticesPerSecond + z.edgesPerSecond, "Policy", "Edges+Vertices/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("evps")))
    println(x.createFigureVerticesPerSecondFigure(_.edgesPerSecond, "Policy", "Edges/s", s"${x.algorithm} on ${x.dataSet}", namingTemplate("eps")))
    println(x.createFigureVerticesPerSecondFigure(_.processingTime, "Policy", "Processing time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("proc")))
    println(x.createFigureVerticesPerSecondFigure(_.machineTime, "Policy", "Machine time (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("machine")))
    println(x.createFigureVerticesPerSecondFigure(_.elasticityOverhead, "Policy", "Elasticity overhead (s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("elasticity-overhead")))
    println(x.createFigureVerticesPerSecondFigure(_.makeSpan, "Policy", "Makespan(s)", s"${x.algorithm} on ${x.dataSet}", namingTemplate("mk")))
  }
}
